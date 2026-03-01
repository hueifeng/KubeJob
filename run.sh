# 查找可能已在运行的 KubeJob Sample 进程并杀掉（防止端口冲突）
lsof -ti:5041 | xargs kill -9 2>/dev/null || true

# 运行 Sample
dotnet run --project samples/KubeJob.Sample.Unified/KubeJob.Sample.Unified.csproj
