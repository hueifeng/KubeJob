# async/await只是语法糖吗？--从Runtime-Async重新认识.NET异步方法的实现

## 摘要

很多人都说 `async/await` 只是语法糖。这个说法并不能算错，但如果只停留在这个层面，恐怕很难真正理解 .NET 异步编程的本质。随着 .NET Runtime 团队开始推进 **Runtime-Async**，async 方法的实现重心正在从编译器侧逐步向 Runtime 侧迁移。本文尝试从编译器状态机、ILSpy 验证、Runtime-Async 设计草案、JIT、桥接逻辑、恢复入口、预编译支持以及诊断支持，以及 Green Thread 实验的取舍出发，重新认识 .NET 异步方法究竟是谁在实现，以及 Runtime-Async 为什么值得关注。

## 关键词

`.NET`、`C#`、`async`、`await`、`Task`、`ValueTask`、`Runtime-Async`、`CLR`、`JIT`、`ILSpy`

很多人都知道，C# 的 `async/await` 是“语法糖”。

这个说法不能算错，但如果只停留在这个层面，恐怕很难真正理解 .NET 异步编程的本质。更重要的是，当 .NET Runtime 团队开始推进所谓的 **Runtime-Async** 特性时，这个“语法糖”的说法就显得不那么充分了。

因为 Runtime-Async 想解决的问题恰恰是：**如果 async 方法不再主要依赖编译器重写状态机，而是由 Runtime 原生支持，会发生什么？**

这不是一个小修小补的优化点，也不是某种新的 API 设计，而是对当前 .NET 异步模型实现方式的一次重新审视。

本文试图回答这样几个问题：

> 一、今天的 async 到底是谁在实现？  
> 二、Runtime-Async 想改变什么？  
> 三、它为什么不只是一个“更快的 await”？  
> 四、它和 Green Thread 到底是什么关系？  
> 五、我们能不能通过代码和工具验证这件事？

---

## 一、我们熟悉的 async，真的属于 Runtime 吗？

先看一段最普通不过的代码：

```csharp
static async Task<int> FooAsync()
{
    await Task.Delay(1000);
    return 1;
}
```

从源码上看，这只是一个带有 `async` 修饰符的方法。  
但是从编译结果来看，它并不是一个“普通方法”。

C# 编译器会在背后做很多事情，包括：

- 生成一个状态机类型
- 将方法体拆分成多个状态段
- 通过 `MoveNext` 驱动执行过程
- 使用编译器生成的 `AsyncTaskMethodBuilder` 管理结果和后续要执行的逻辑

也就是说，今天我们写下的 `async` 方法，本质上更像是：

**编译器将一种高级语言语义翻译成 CLR 现有能力之上的一套约定。**

所以如果严格一点说，当前 async 的“第一实现者”其实不是 Runtime，而是编译器。

这也是为什么“`async/await` 是语法糖”这个说法会广泛流传。  
但这里所谓的“糖”，显然不是简单的文本替换，而是一次相当重的编译期变换。

---

## 二、一个 async 方法编译之后会变成什么样子？

为了让问题更直观，我们先来看一个稍微完整一点的例子：

```csharp
using System;
using System.Threading.Tasks;

class Program
{
    static async Task<int> AddAsync(int x, int y)
    {
        await Task.Delay(100);
        return x + y;
    }

    static async Task Main()
    {
        var result = await AddAsync(1, 2);
        Console.WriteLine(result);
    }
}
```

从写法上看，`AddAsync` 像是“暂停一下，再返回结果”。  
但编译器大致会将它改写成一种类似这样的结构（伪代码）：

```csharp
[CompilerGenerated]
private struct <AddAsync>d__0 : IAsyncStateMachine
{
    public int _state;
    public AsyncTaskMethodBuilder<int> _builder;
    public int x;
    public int y;

    private TaskAwaiter _awaiter;

    public void MoveNext()
    {
        try
        {
            int result;

            if (_state == 0)
            {
                goto ResumeAfterAwait;
            }

            var awaiter = Task.Delay(100).GetAwaiter();
            if (!awaiter.IsCompleted)
            {
                _state = 0;
                _awaiter = awaiter;
                _builder.AwaitUnsafeOnCompleted(ref awaiter, ref this);
                return;
            }

            _awaiter = awaiter;

        ResumeAfterAwait:
            _awaiter.GetResult();
            result = x + y;
            _builder.SetResult(result);
        }
        catch (Exception ex)
        {
            _builder.SetException(ex);
        }
    }

    public void SetStateMachine(IAsyncStateMachine stateMachine) { }
}
```

对熟悉 `async/await` 的开发者来说，这种结构并不陌生。  
但它非常明确地说明了一件事：

**今天的 async 方法，运行时看到的并不是源码里那个样子，而是一套由编译器提前生成好的状态机。**

换句话说，Runtime 并不真正“原生理解”这个方法是 async 方法。  
它只是在执行编译器产出的结果。

---

## 三、先用几段代码把“今天的 async”看清楚

如果后文要谈 Runtime-Async，那么前提是先把“传统 async”看清楚。下面几组代码都很简单，但每一组都对应一个很重要的观察点。

### 示例一：反射观察 async 方法的表象

```csharp
using System;
using System.Reflection;
using System.Runtime.CompilerServices;
using System.Threading.Tasks;

public static class AsyncReflectionDemo
{
    public static async Task<int> AddAsync(int x, int y)
    {
        await Task.Delay(10);
        return x + y;
    }

    public static async ValueTask<int> AddValueAsync(int x, int y)
    {
        await Task.Yield();
        return x + y;
    }
}

public class Program
{
    public static async Task Main()
    {
        Dump(nameof(AsyncReflectionDemo.AddAsync));
        Dump(nameof(AsyncReflectionDemo.AddValueAsync));

        Console.WriteLine(await AsyncReflectionDemo.AddAsync(1, 2));
        Console.WriteLine(await AsyncReflectionDemo.AddValueAsync(3, 4));
    }

    private static void Dump(string methodName)
    {
        var method = typeof(AsyncReflectionDemo).GetMethod(
            methodName,
            BindingFlags.Public | BindingFlags.Static)!;

        Console.WriteLine($"Method: {method}");
        Console.WriteLine($"返回类型: {method.ReturnType}");
        Console.WriteLine($"方法实现标志（MethodImplFlags）: {method.GetMethodImplementationFlags()}");
        Console.WriteLine($"是否带有 AsyncStateMachineAttribute: {method.IsDefined(typeof(AsyncStateMachineAttribute), false)}");

        var attr = method.GetCustomAttribute<AsyncStateMachineAttribute>();
        Console.WriteLine($"状态机类型: {attr?.StateMachineType}");
        Console.WriteLine();
    }
}
```

这段代码的价值在于它揭示了今天 async 方法在元数据层面的一种典型表象：

- 方法本身仍然是普通方法
- 返回类型仍然是 `Task<T>` 或 `ValueTask<T>`
- 额外有一个 `AsyncStateMachineAttribute`
- 可以拿到编译器生成的状态机类型

这说明一件很重要的事：

**今天 async 的核心语义，并没有成为 Runtime 对“方法身份”的原生描述，而是通过“普通方法 + 编译器属性 + 生成状态机”这套组合表达出来的。**

### 示例二：`StackTrace` 为什么总让人觉得“不够用”？

```csharp
using System;
using System.Threading.Tasks;

public static class StackTraceDemo
{
    public static async Task EntryAsync()
    {
        await OuterAsync();
    }

    private static async Task OuterAsync()
    {
        await MiddleAsync();
    }

    private static async Task MiddleAsync()
    {
        await Task.Delay(10);

        Console.WriteLine("=== Environment.StackTrace ===");
        Console.WriteLine(Environment.StackTrace);

        throw new InvalidOperationException("boom");
    }
}

public class Program
{
    public static async Task Main()
    {
        try
        {
            await StackTraceDemo.EntryAsync();
        }
        catch (Exception ex)
        {
            Console.WriteLine("=== Exception.ToString() ===");
            Console.WriteLine(ex);
        }
    }
}
```

很多人第一次认真看这类输出时都会产生一种直觉：

- 我明明是从 `EntryAsync -> OuterAsync -> MiddleAsync` 走下来的
- 为什么当前线程栈里却经常夹杂着线程池、awaiter、dispatch 之类的内部实现细节？

原因并不复杂：

- 线程上的“物理调用栈”
- 和开发者心中的“逻辑异步调用链”
- 并不是同一回事

传统 async 的逻辑链路很大一部分藏在：

- Roslyn 生成的状态机
- 后续执行逻辑的注册过程
- builder / awaiter 这一整套语义里

Runtime 并不天然掌握一条完整的“逻辑调用链”。

### 示例三：为什么 `AsyncLocal` / `ExecutionContext` 是个大坑？

```csharp
using System;
using System.Threading;
using System.Threading.Tasks;

public static class AsyncLocalDemo
{
    private static readonly AsyncLocal<string?> Context = new();

    public static async Task RunAsync()
    {
        Context.Value = "outer";
        Console.WriteLine($"Outer before await: {Context.Value}");

        await InnerAsync();

        Console.WriteLine($"Outer after inner returns: {Context.Value}");
    }

    private static async Task InnerAsync()
    {
        Console.WriteLine($"Inner before mutation: {Context.Value}");

        Context.Value = "inner";
        Console.WriteLine($"Inner after mutation: {Context.Value}");

        await Task.Delay(10);

        Console.WriteLine($"Inner after await: {Context.Value}");
    }
}

public class Program
{
    public static async Task Main()
    {
        await AsyncLocalDemo.RunAsync();
    }
}
```

这段代码的重点不是“演示今天 async 有问题”，而是说明：

**一旦 Runtime 想接管 async 的挂起 / 恢复执行语义，就必须对 `ExecutionContext` / `AsyncLocal` 的行为负责。**

### 示例四：`SynchronizationContext` 和恢复点为什么不是“语法问题”？

```csharp
using System;
using System.Threading;
using System.Threading.Tasks;

public sealed class LoggingSynchronizationContext : SynchronizationContext
{
    public override void Post(SendOrPostCallback d, object? state)
    {
        Console.WriteLine($"Post called on thread {Environment.CurrentManagedThreadId}");

        ThreadPool.QueueUserWorkItem(_ =>
        {
            Console.WriteLine($"Continuation running on thread {Environment.CurrentManagedThreadId}");
            d(state);
        });
    }
}

public class Program
{
    public static async Task Main()
    {
        SynchronizationContext.SetSynchronizationContext(new LoggingSynchronizationContext());

        Console.WriteLine($"Before await: thread {Environment.CurrentManagedThreadId}");
        await Task.Yield();
        Console.WriteLine($"After await: thread {Environment.CurrentManagedThreadId}");

        Console.WriteLine("---- ConfigureAwait(false) ----");

        Console.WriteLine($"Before delay: thread {Environment.CurrentManagedThreadId}");
        await Task.Delay(10).ConfigureAwait(false);
        Console.WriteLine($"After delay: thread {Environment.CurrentManagedThreadId}");
    }
}
```

这段代码非常适合说明一个常被忽视的事实：

**await 之后“去哪里恢复”，不是语法糖自动完成的神秘魔法，而是一套具体的上下文与调度规则。**

### 示例五：为什么 `ref struct` 不能轻易跨 await？

下面这段代码是故意“写坏”的：

```csharp
using System;
using System.Threading.Tasks;

public static class RefStructDemo
{
    public static async Task<int> LengthAsync(string text)
    {
        ReadOnlySpan<char> span = text.AsSpan();

        await Task.Yield(); // 编译错误：span 不能跨 await 保存

        return span.Length;
    }
}
```

这段代码的重要性在于：

- 它把 `await` 从“暂停一下”还原成了一个真正的语义边界
- 一旦发生挂起，编译器/JIT/Runtime 就必须决定：
  - 哪些局部变量能跨过去
  - 哪些不能跨过去
  - 哪些值需要在恢复执行时继续保留
  - 哪些值根本不能安全地跨过 `await`

### 示例六：一个最小基准，说明为什么 Runtime 团队会认真做这件事

```csharp
using System;
using System.Diagnostics;
using System.Threading.Tasks;

public static class BenchDemo
{
    public static async Task<long> BaselineAsync(int count)
    {
        long sum = 0;
        for (int i = 0; i < count; i++)
        {
            await Task.Yield();
            sum += i;
        }
        return sum;
    }
}

public class Program
{
    public static async Task Main()
    {
        var sw = Stopwatch.StartNew();
        var result = await BenchDemo.BaselineAsync(10000);
        sw.Stop();

        Console.WriteLine($"Result: {result}");
        Console.WriteLine($"Elapsed: {sw.ElapsedMilliseconds} ms");
    }
}
```

这段代码本身当然不是什么权威 benchmark，但它足够让读者意识到：

- async 的热路径成本是真实存在的
- 大量 `await` 的成本、异常传播成本、上下文成本，都不是“想象中的问题”

---

## 四、借助 ILSpy 观察一个普通 async 方法

如果只是停留在“async/await 是语法糖”这句话上，其实还是有点抽象。  
更直接的办法，是把一个普通的 async 方法丢给 ILSpy 看看它到底变成了什么。

我们还是使用前面的 `AddAsync` 例子。

### 1. 先用 Release 模式编译

```bash
dotnet build -c Release
```

### 2. 用 ILSpy 打开程序集后重点看什么？

对于 `AddAsync`，建议重点观察以下内容：

- 原始方法本身
- 编译器生成的嵌套状态机类型
- `MoveNext` 方法
- `SetStateMachine`
- `AsyncTaskMethodBuilder<int>`

如果是一个传统的、由编译器生成状态机的 async 方法，通常能看到如下特征。

#### 特征一：存在一个编译器生成的状态机类型

类似这种名字：

```text
<AddAsync>d__0
```

这已经非常能说明问题了：  
所谓的 `AddAsync`，实际上被翻译成了一个实现 `IAsyncStateMachine` 的状态机类型。

#### 特征二：原始方法只是“启动状态机”的壳

ILSpy 里看到的 `AddAsync` 往往已经不再是“真正执行业务逻辑”的方法，而更像一个状态机启动器。它大致会做这些事情：

- 创建状态机实例
- 初始化参数
- 创建 `AsyncTaskMethodBuilder<int>`
- 设置状态为初始值
- 调用 `Start`
- 返回 `builder.Task`

#### 特征三：真正的逻辑在 `MoveNext`

`await Task.Delay(10)`、恢复执行、返回 `x + y` 这些行为，真正都在 `MoveNext` 里面。

### 3. ILSpy 验证的真正意义是什么？

通过 ILSpy，我们其实验证了这样一条非常重要的结论：

**今天 async 的主要结构化语义不是由 Runtime 原生提供的，而是由编译器通过状态机改写先行编码出来的。**

这就是整篇文章后面要讨论 Runtime-Async 的前提。

---

## 五、Runtime-Async 到底想改变什么？

到这里，真正的问题才开始。

如果说今天的 async 是“编译器把方法改写成状态机”，那么 Runtime-Async 到底要做什么？

很多文章会把这件事简单概括成一句话：

> “让 Runtime 直接支持 async 方法。”

这句话不算错，但其实远远不够。  
因为 Runtime-Async 想改变的，并不仅仅是代码生成位置，而是 **async 在 CLR 中的存在形式**。

更准确地说，它至少试图同时改变下面三件事情：

- async 方法的**元数据身份**
- async 方法的**调用约定**
- async 方法的**挂起/恢复执行语义归属**

---

## 六、第一层变化：它试图让 async 成为“方法的一种形态”

根据公开设计草案，Runtime-Async 最关键的入口是：

```csharp
[MethodImpl(MethodImplOptions.Async)]
```

这件事看起来像个细节，实际上非常关键。

今天的 async 在 Runtime 眼里，本质上还是：

- 一个普通方法
- 外加一些编译器生成的 supporting types

但 Runtime-Async 的设计是在方法定义层面直接引入一个新的概念：

- method may be either `sync` or `async`

这意味着什么？

意味着 async 不再只是源代码层面的修饰，也不只是编译器内部 lowering 规则，而是成为 Runtime 可识别的方法种类。

这和今天最大的区别就在于：

- 今天的 async 是“编译器先展开，Runtime 再执行”
- Runtime-Async 试图变成“Runtime 直接理解 async 方法本身”

这个变化非常像把“async”从一个**实现技巧**升级为一个**运行时语义标签**。

而且这不是猜测，设计草案里写得很明确：

- `MethodImplAttributes.Async = 0x2000`
- `ilasm` / `ildasm` 将识别 `async` 关键字
- async 方法在 ECMA-335 草案中被单独定义

这说明 Runtime-Async 并不是偷偷摸摸地在 JIT 里加一点优化，而是在试图修改 **CLI 对方法的描述方式**。

---

## 七、第二层变化：它试图改变 async 的调用约定

如果只是给方法打个标记，这件事其实还谈不上“深刻”。  
真正深刻的地方在于：Runtime-Async 不只是让方法“看起来不同”，而是让方法“调用起来不同”。

在 runtimelab 的实验文档里，最值得注意的一点是：

- 实验阶段的 runtime-async 方法（文档里常记作 async2）
- 可以通过 **不同的入口形式** 被调用

也就是说，一个逻辑上的 async 方法，不再只有今天这种“返回 Task”的单一表象。  
从 Runtime 的视角看，它可能同时存在：

- 返回 `Task` 的入口
- runtime-async 入口
- 恢复入口辅助函数

这也是为什么后续会出现很多看起来非常“底层”的工程问题，比如：

- 桥接逻辑（thunk）
- 恢复入口辅助函数
- ReadyToRun（预编译） 预编译结果中如何存放这些入口
- 反射如何隐藏这些实现细节
- StackTrace 如何展示这些逻辑帧

从 `dotnet/runtime#121559` 的描述里可以看到，单个 async 方法在 ReadyToRun 场景下甚至可能需要：

- Task 返回入口
- async 调用入口
- 恢复入口辅助函数

这说明 Runtime-Async 改的不是“某个 helper 的实现”，而是：

**一个方法在 Runtime 中究竟以几种身份存在，分别服务于什么调用路径。**

---

## 八、为什么会有适配桩和恢复入口？

这是 Runtime-Async 最容易“听说过”，但最容易“没真正想明白”的地方。

### 1. 什么是桥接逻辑（thunk）？

简单说，`thunk` 就是一层桥接逻辑，也可以把它理解为一层中转入口。  
之所以需要它，是因为 Runtime-Async 想做到：

- 尽量兼容今天的 `Task/ValueTask` 世界
- 但又允许 Runtime 内部存在新的 async 调用约定

所以就会出现两类桥接调用：

- `async1 -> async2`
- `async2 -> async1`

这里的“async1”可以粗略理解为今天这种由编译器生成状态机的 async；  
“async2”则是 runtimelab 在实验阶段对 runtime-async 语义使用的内部称呼。

### 2. runtime-async 调用传统 `Task` 方法时的伪代码

这是实验文档里最容易帮助理解的伪代码之一：

```csharp
async2 Task<ReturnType> Thunk(ParameterType param1, ParameterType2 param2, ...)
{
    var awaiter = TargetMethod(param1, param2, ...).GetAwaiter();
    if (!awaiter.IsCompleted)
    {
        RuntimeHelpers.UnsafeAwaitAwaiterFromRuntimeAsync(awaiter);
    }
    return awaiter.GetResult();
}
```

这个伪代码说明了一件事：

- runtime-async 并没有抛弃 awaiter 这套模型
- 它仍然要和现有 `Task`/`ValueTask` 生态打通
- 只是“挂起”的处理位置从编译器模板代码转到了 Runtime 参与的路径中

### 3. 传统 `Task` 世界调用 runtime-async 方法时的伪代码

反过来，传统的 `Task` 世界也需要能够调用 runtime-async 方法。实验文档里的伪代码大致是这样：

```csharp
static Task<int> FooAdapter(int a, int b)
{
    int result;
    Continuation? continuation = null;

    try
    {
        result = Foo(a, b); // runtime-async 入口
        continuation = StubHelpers.AsyncCallContinuation();
    }
    catch (Exception ex)
    {
        return Task.FromException<int>(ex);
    }

    if (continuation == null)
    {
        return Task.FromResult(result);
    }

    return FinalizeTaskReturningThunk<int>(continuation);
}
```

这段伪代码所揭示的深层事实是：

**Runtime-Async 并不是单一的“新方法体实现”，而是在构造一个能够与旧 async 世界双向互通的桥接层。**

### 4. 什么是恢复入口（resumption stub）？

如果一个 async 方法发生了挂起，那么将来恢复执行时，必须有一个恢复入口。  
这个恢复入口不是源代码里直接写出来的，而是 Runtime/JIT 帮你搭出来的。这个恢复入口在实现层面通常叫 `resumption stub`。如果用更自然一点的话来说，它就是一段专门负责把执行流程接回目标方法的恢复入口代码。

从实验文档看，它大致承担这些职责：

- 带着 continuation 对象重新进入目标方法
- 以正确方式补齐默认参数或恢复状态
- 处理返回值如何回写到下一层 continuation
- 如果再次挂起，则继续向外返回 continuation

示意伪代码大概像这样：

```csharp
static Continuation? ResumeFoo(Continuation continuation)
{
    int result = Foo(continuation, 0, 0); // 走恢复入口
    Continuation? next = StubHelpers.AsyncCallContinuation();

    if (next == null)
    {
        Unsafe.Write(ref continuation.Next.Data[index], result);
    }

    return next;
}
```

这类代码非常重要，因为它能让我们意识到：

- Runtime-Async 改的不是某个 `await` helper
- 它在改“方法怎么调用、怎么恢复、怎么和旧世界互通”

---

## 九、第三层变化：暂停与恢复不再只是编译器状态机的私事

传统的、由编译器驱动的 async，其核心机制是：

- 编译器把方法拆成状态机
- `await` 变成：
  - `GetAwaiter`
  - `IsCompleted`
  - 注册后续执行逻辑
  - `GetResult`

而 Runtime-Async 的设计是：

- 挂起点成为 Runtime 理解的语义
- Runtime 自己掌握恢复点、continuation 以及恢复入口之间的关系

这件事带来的深层影响有两个。

### 1. Runtime 可以直接跟踪“逻辑上的异步调用链”

这正是 `dotnet/runtime#125417` 讨论的重点。  
该 issue 想做的是：

- 在 `Environment.StackTrace`
- 以及 `System.Diagnostics.StackFrame`

里纳入 runtime async 对应的调用帧

为什么这件事以前为什么难做？

因为在传统 async 中，逻辑调用链主要藏在：

- Roslyn 生成的状态机
- 编译器生成的 Builder
- 后续执行链路

Runtime 并不天然知道“逻辑上的 caller 是谁”。

而 Runtime-Async 则不同。  
因为 async 恢复链路本来就由 Runtime 掌握，所以 Runtime 才有可能把：

- 物理线程栈
- 逻辑异步调用链

重新拼成更符合开发者认知的调用栈。

也就是说，Runtime-Async 带来的变化不仅是性能，还包括：

**可诊断性的语义重建。**

### 2. Runtime 可以重新定义哪些状态该跨挂起点保存

设计草案里还有一个非常有意思但经常被忽略的点：

- 跨挂起点之后仍然会被使用的局部变量，需要被额外保存下来
- by-ref locals 不能被安全地这样保存
- byref-like struct 也不能随便跨挂起点保留

如果换一种更容易理解的说法，就是：

- `await` 之前的某些局部变量，如果 `await` 之后还要继续使用，就必须被“带到恢复点之后”
- 但并不是所有值都适合这样做
- 尤其是那些和当前栈帧强绑定的值，一旦方法挂起，原来的栈上下文就不再可靠了

例如下面这段代码可以正常工作：

```csharp
static async Task<int> OkAsync()
{
    int x = 10;
    await Task.Delay(1);
    return x + 1;
}
```

这里的 `x` 在 `await` 之后还会被用到，所以它必须被额外保存起来，否则方法恢复时就找不到原来的值了。

但下面这段代码就不行：

```csharp
static async Task<int> BadAsync()
{
    Span<int> buffer = stackalloc int[1];
    await Task.Delay(1);
    return buffer[0];
}
```

原因不在于 `Span<T>` 不能保存数据，而在于它指向的是当前方法栈上的内存。方法一旦在 `await` 处挂起，原来的栈帧就不能再被当作长期有效的存储区域。因此这类值不是“保存起来比较麻烦”，而是**根本不能被安全地跨挂起点保留**。

这一点会直接影响：

- liveness analysis
- dead code elimination
- zeroing
- 后续执行状态的大小
- GC 可见状态

这不是应用层开发者平时会关心的事，但它恰恰说明 Runtime-Async 触碰的是编译器和 JIT 共同维护的“程序活性语义”。

相关 issue 里甚至专门提到：

- `#115261`
  - 挂起感知的活性分析与死代码消除（suspension-aware liveness and dead-code elimination）
- `#115263`
  - 避免在恢复执行后继续持有旧的 continuation 状态

这意味着 Runtime-Async 不只是换一种保存状态的方式，而是在重构：

**“程序在挂起/恢复执行边界上，哪些值还算活着”**

---

## 十、Runtime-Async 的探索并不只有一条路线

这部分特别重要，因为很多人会误以为 Runtime-Async 从一开始就只有一种实现方案。  
实际上，公开实验文档至少体现了两条原型路线。

### 路线一：基于 unwinder 的原型

这条路线的核心思想是：

- 正常执行时尽量像同步代码
- 真正发生挂起时，借助 unwind 把栈帧“拆下来”
- 把这些栈帧以 `Tasklet` 一类的数据结构保存起来
- 恢复时再把这些状态重新铺回去

这条路线最吸引人的地方在于，它很像在说：

> “与其在编译期显式制造状态机，不如把正在执行的调用栈本身当作状态机。”

它的优点很明显：

- 不发生挂起时非常接近同步代码路径
- 理论上对 byref / ref struct 更自然
- 某些语义上更接近“真正暂停一个函数”

但问题也同样明显：

- 挂起成本高
- GC 需要理解这些 tasklet 状态（可以把它理解为被拆分保存下来的栈帧状态）
- EH/stackwalking/debugging 都会变复杂
- 工程维护成本很高

公开实验结论后来也明确表示：

- 两种原型都跑了
- 团队最终更看好 **JIT implementation**
- 无论在性能还是维护性上，JIT 路线都更有吸引力

### 路线二：基于 JIT 生成状态机的原型

这条路线更接近现在主推方向。

它没有走“真正拆栈”的重路线，而是让：

- JIT 在 async2 方法里生成挂起 / 恢复执行逻辑
- 运行时通过 `Continuation` 对象保存恢复状态
- 每个挂起点有相应的恢复入口
- Runtime 和 JIT 一起负责后续执行链路

实验文档里给出的 `Continuation` 大致长这样：

```csharp
internal sealed unsafe class Continuation
{
    public Continuation? Next;
    public delegate*<Continuation, Continuation?> Resume;
    public uint State;
    public CorInfoContinuationFlags Flags;
    public byte[]? Data;
    public object[]? GCData;
}
```

这非常能说明问题：

- 它不是传统 Roslyn 状态机模板
- 但它也不是 Green Thread
- 它更像 Runtime/JIT 协作生成的一种“基于 continuation 的状态机”

---

## 十一、它为什么不只是“更快的 await”？

如果只从表面看，Runtime-Async 最容易被理解成：

- 少分配一点对象
- 少生成一些状态机代码
- await 更快一点

这些当然都重要，但如果只看到这里，就会低估它的意义。

真正值得关注的是：

### 1. 它在试图改变 async 的“归属权”

过去 async 的主舞台在：

- Roslyn
- 编译器生成的 `AsyncTaskMethodBuilder`
- 编译器状态机

而 Runtime-Async 想做的是：

- 把一部分核心语义收回 Runtime

### 2. 它在试图减少“源码语义”和“执行语义”的距离

今天的 async 方法：

- 写起来像普通方法
- 编译后却变成状态机 + builder + continuation 模板代码

Runtime-Async 的目标之一，就是让 async 方法在 Runtime 视角下更接近它本来的语义。

### 3. 它在为 JIT、反射、诊断争取原生话语权

只要 async 语义主要藏在编译器生成代码里，Runtime 很多事情只能“事后适配”。  
而一旦 async 语义被 Runtime 原生掌握，那么：

- StackTrace
- Reflection
- ReadyToRun（预编译）
- PGO
- Native AOT
- 跨架构支持

都可能变成“第一性支持”，而不是“兼容性支持”。

---

## 十二、它和 Green Thread 的真正关系是什么？

很多人会觉得，既然 Runtime-Async 都已经做到这个深度了，那是不是其实就是换一种方式去做 Green Thread？

答案仍然是否定的。

官方在 `.NET 9 Runtime Async Experiment` 里已经写得非常清楚：

- .NET 8 做过 Green Thread 相关实验
- 学到了很多
- 但当前不继续推进
- 随后转向 Runtime-Async

也就是说：

- Green Thread 不是今天 Runtime-Async 的另一个名字
- Runtime-Async 也不是 Green Thread 的轻量版
- 它们是两条方向不同、目标层次不同的路线

最本质的区别依然是：

- Green Thread 试图重构**线程与调度模型**
- Runtime-Async 试图重构**异步方法的实现模型**

---

## 十三、我们能不能写一个 Runtime-Async Demo 观察一下？

可以。  
不过需要先说明一点：Runtime-Async 当前仍然属于预览/实验性质的运行时能力，所以它不像普通 API 那样“写几行代码立刻就能看出差异”。更准确地说，这个 Demo 的重点不是业务行为的差异，而是：

**相同的源码，在不同的编译与运行配置下，底层实现路径正在发生变化。**

### 1. 最小控制台程序

`Program.cs`

```csharp
using System;
using System.Reflection;
using System.Runtime.CompilerServices;
using System.Threading.Tasks;

Console.WriteLine("=== Runtime-Async Demo ===");

var method = typeof(Demo).GetMethod(nameof(Demo.AddAsync))!;
Console.WriteLine($"Method: {method}");
Console.WriteLine($"方法实现标志（MethodImplFlags）: {method.GetMethodImplementationFlags()}");

var result = await Demo.AddAsync(1, 2);
Console.WriteLine($"AddAsync(1, 2) = {result}");

var sum = await Demo.ManyAwaitsAsync(5);
Console.WriteLine($"ManyAwaitsAsync(5) = {sum}");

static class Demo
{
    [MethodImpl(MethodImplOptions.NoInlining)]
    public static async Task<int> AddAsync(int x, int y)
    {
        await Task.Delay(100);
        return x + y;
    }

    [MethodImpl(MethodImplOptions.NoInlining)]
    public static async Task<int> ManyAwaitsAsync(int count)
    {
        var sum = 0;
        for (var i = 0; i < count; i++)
        {
            await Task.Yield();
            sum += i;
        }

        return sum;
    }
}
```

### 2. 项目文件

`RuntimeAsyncDemo.csproj`

```xml
<Project Sdk="Microsoft.NET.Sdk">
  <PropertyGroup>
    <OutputType>Exe</OutputType>
    <TargetFramework>net10.0</TargetFramework>
    <LangVersion>preview</LangVersion>
    <EnablePreviewFeatures>true</EnablePreviewFeatures>
    <Features>$(Features);runtime-async=on</Features>
  </PropertyGroup>
</Project>
```

### 3. 运行方式

构建：

```bash
dotnet build
```

运行：

```bash
DOTNET_RuntimeAsync=1 dotnet run
```

### 4. 这个 Demo 真正说明了什么？

它最有意思的地方不在于输出结果，而在于：

- 业务代码没有变化
- 还是同样的 `async/await`
- 但 Runtime 团队已经开始尝试让这段代码走向不同的底层实现

---

## 十四、为什么它现在还没彻底成熟？

一项 Runtime 级特性真正难的地方，从来不是“做个 Demo 跑通”，而是：

**把它放进整个 Runtime 生态后，所有边角都得能工作。**

从当前公开 issue 看，Runtime-Async 仍然有这些典型未完成项：

### 1. ReadyToRun（R2R，预编译）

- `#115098`
- `#121559`

这说明：

- async 方法不只是需要一个本地 JIT 入口
- 还要考虑预编译镜像中怎样存放多个入口和恢复入口辅助函数

### 2. Native AOT

- `#124101`

这说明：

- Runtime-Async 不能只在 JIT 世界里成立
- 还必须能适应 AOT 世界的代码布局、恢复信息和运行时支持

### 3. PGO

- `#115096`

这说明：

- Runtime-Async 不仅要能跑
- 还要能被 profile-guided optimization 正确看见和利用

### 4. Reflection / Diagnostics

- `#115099`
- `#125417`

### 5. ExecutionContext / AsyncLocal 的语义兼容问题

- `#122052`

这几个问题共同说明：

**Runtime-Async 已经不是一个概念想法，但距离“完全透明替换今天的 async”仍然有工程距离。**

---

## 十五、我们应该如何重新理解 async？

回到文章开头的问题：

**async/await 只是语法糖吗？**

如果从今天绝大多数 .NET 程序的实现方式来看，这个说法大体没有问题。  
因为 async 的主要语义确实是由编译器生成状态机来承载的。

但如果从 .NET Runtime 的演进趋势来看，这句话已经显得不够完整了。

因为 Runtime-Async 正在说明一件事情：

**async 并不只是一种语言层面的便利写法，它正在逐步成为 Runtime 原生理解和承载的执行语义。**

如果说 ILSpy 让我们看清了今天的 async 本质上是一种编译器状态机，那么 Runtime-Async 的价值就在于：

**它试图把这种状态机所表达的语义重新收归到 Runtime。**

真正值得关注的，不只是它能不能减少几次分配，或者让 `await` 快几个百分点，而是它正在尝试改变 async 在 .NET 中的“归属权”。

---

## 十六、结语

很长一段时间里，我们讨论 .NET 异步编程，关注点大多停留在这些问题上：

- 为什么不能随便用 `.Result`
- `Task.Run` 到底会不会新开线程
- `ConfigureAwait(false)` 到底要不要写
- 为什么异步方法会有状态机

这些问题当然重要，但它们更多属于“如何使用 async”。

而 Runtime-Async 让我们有机会重新思考另一个更底层的问题：

**async 到底应该由谁来实现？**

如果说过去的答案是“主要由编译器实现”，那么 Runtime 团队现在给出的方向是：

**也许，它本来就应该属于 Runtime。**

---

## 参考资料

- `.NET Runtime-Async Feature`
- `.NET 9 Runtime Async Experiment`
- `docs/design/specs/runtime-async.md`
- runtimelab `runtime-handled-tasks.md`
- `dotnet/runtime#115093`
- `dotnet/runtime#115099`
- `dotnet/runtime#115098`
- `dotnet/runtime#124101`
- `dotnet/runtime#115096`
- `dotnet/runtime#125417`
- `dotnet/runtime#122052`
- `dotnet/runtime#121559`

> 本文中的 Runtime-Async 细节基于公开 issue、设计草案和 runtimelab 实验文档整理，预览版本中的实现和配置方式后续可能会发生调整。
