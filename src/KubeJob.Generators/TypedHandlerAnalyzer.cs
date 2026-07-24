using System.Collections.Immutable;
using System.Linq;
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.Diagnostics;

namespace KubeJob.Generators;

[DiagnosticAnalyzer(LanguageNames.CSharp)]
public sealed class TypedHandlerAnalyzer : DiagnosticAnalyzer
{
    private const string AttributeMetadataName = "KubeJob.Core.Attributes.KubeJobAttribute";
    private const string GenericJobMetadataName = "KubeJob.Core.Interfaces.IKubeJob`1";

    private static readonly DiagnosticDescriptor TypedHandlerRequired = new(
        id: "KJGEN001",
        title: "KubeJob handler must be strongly typed",
        messageFormat: "Handler '{0}' declares [KubeJob] but does not implement IKubeJob<TPayload>",
        category: "KubeJob.Generators",
        defaultSeverity: DiagnosticSeverity.Error,
        isEnabledByDefault: true);

    public override ImmutableArray<DiagnosticDescriptor> SupportedDiagnostics =>
        ImmutableArray.Create(TypedHandlerRequired);

    public override void Initialize(AnalysisContext context)
    {
        context.ConfigureGeneratedCodeAnalysis(GeneratedCodeAnalysisFlags.None);
        context.EnableConcurrentExecution();
        context.RegisterCompilationStartAction(startContext =>
        {
            var attributeType = startContext.Compilation.GetTypeByMetadataName(AttributeMetadataName);
            var genericJobType = startContext.Compilation.GetTypeByMetadataName(GenericJobMetadataName);
            if (attributeType is null || genericJobType is null)
            {
                return;
            }

            startContext.RegisterSymbolAction(symbolContext =>
            {
                var handler = (INamedTypeSymbol)symbolContext.Symbol;
                if (handler.TypeKind != TypeKind.Class
                    || handler.IsAbstract
                    || !handler.GetAttributes().Any(attribute =>
                        SymbolEqualityComparer.Default.Equals(attribute.AttributeClass, attributeType)))
                {
                    return;
                }

                var isTyped = handler.AllInterfaces.Any(interfaceType =>
                    SymbolEqualityComparer.Default.Equals(
                        interfaceType.OriginalDefinition,
                        genericJobType));
                if (isTyped)
                {
                    return;
                }

                symbolContext.ReportDiagnostic(Diagnostic.Create(
                    TypedHandlerRequired,
                    handler.Locations.FirstOrDefault(location => location.IsInSource),
                    handler.ToDisplayString(SymbolDisplayFormat.CSharpErrorMessageFormat)));
            }, SymbolKind.NamedType);
        });
    }
}
