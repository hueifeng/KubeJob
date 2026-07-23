using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.Diagnostics;

namespace KubeJob.Generators;

[DiagnosticAnalyzer(LanguageNames.CSharp)]
public sealed class DuplicateJobKeyAnalyzer : DiagnosticAnalyzer
{
    private const string AttributeMetadataName = "KubeJob.Core.Attributes.KubeJobAttribute";

    private static readonly DiagnosticDescriptor DuplicateJobKey = new(
        id: "KJGEN003",
        title: "Duplicate stable KubeJob key",
        messageFormat: "Job key '{0}' is declared by multiple handlers: {1}",
        category: "KubeJob.Generators",
        defaultSeverity: DiagnosticSeverity.Error,
        isEnabledByDefault: true,
        customTags: WellKnownDiagnosticTags.CompilationEnd);

    public override ImmutableArray<DiagnosticDescriptor> SupportedDiagnostics =>
        ImmutableArray.Create(DuplicateJobKey);

    public override void Initialize(AnalysisContext context)
    {
        context.ConfigureGeneratedCodeAnalysis(GeneratedCodeAnalysisFlags.None);
        context.EnableConcurrentExecution();

        context.RegisterCompilationStartAction(startContext =>
        {
            var attributeType = startContext.Compilation.GetTypeByMetadataName(AttributeMetadataName);
            if (attributeType is null)
            {
                return;
            }

            var declarations = new ConcurrentDictionary<string, ConcurrentBag<Declaration>>(
                StringComparer.Ordinal);

            startContext.RegisterSymbolAction(symbolContext =>
            {
                var handler = (INamedTypeSymbol)symbolContext.Symbol;
                if (handler.TypeKind != TypeKind.Class)
                {
                    return;
                }

                var attribute = handler.GetAttributes().FirstOrDefault(candidate =>
                    SymbolEqualityComparer.Default.Equals(candidate.AttributeClass, attributeType));
                if (attribute is null
                    || attribute.ConstructorArguments.Length == 0
                    || attribute.ConstructorArguments[0].Value is not string jobKey
                    || string.IsNullOrWhiteSpace(jobKey))
                {
                    return;
                }

                var declaration = new Declaration(
                    handler.ToDisplayString(SymbolDisplayFormat.CSharpErrorMessageFormat),
                    handler.Locations.FirstOrDefault(location => location.IsInSource));
                declarations.GetOrAdd(jobKey, static _ => new ConcurrentBag<Declaration>())
                    .Add(declaration);
            }, SymbolKind.NamedType);

            startContext.RegisterCompilationEndAction(endContext =>
            {
                foreach (var entry in declarations)
                {
                    var handlers = entry.Value
                        .OrderBy(static declaration => declaration.HandlerName, StringComparer.Ordinal)
                        .ToArray();
                    if (handlers.Length < 2)
                    {
                        continue;
                    }

                    var names = string.Join(", ", handlers.Select(static declaration => declaration.HandlerName));
                    foreach (var declaration in handlers)
                    {
                        endContext.ReportDiagnostic(Diagnostic.Create(
                            DuplicateJobKey,
                            declaration.Location,
                            entry.Key,
                            names));
                    }
                }
            });
        });
    }

    private sealed class Declaration
    {
        public Declaration(string handlerName, Location? location)
        {
            HandlerName = handlerName;
            Location = location;
        }

        public string HandlerName { get; }
        public Location? Location { get; }
    }
}
