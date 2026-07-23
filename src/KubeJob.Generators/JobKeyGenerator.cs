using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;
using System.Text;
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp;
using Microsoft.CodeAnalysis.CSharp.Syntax;
using Microsoft.CodeAnalysis.Text;

namespace KubeJob.Generators;

[Generator(LanguageNames.CSharp)]
public sealed class JobKeyGenerator : IIncrementalGenerator
{
    private const string AttributeMetadataName = "KubeJob.Core.Attributes.KubeJobAttribute";
    private const string GenericJobMetadataName = "KubeJob.Core.Interfaces.IKubeJob<TPayload>";

    private static readonly DiagnosticDescriptor MissingPayloadContract = new(
        id: "KJGEN001",
        title: "KubeJob handler must implement IKubeJob<TPayload>",
        messageFormat: "Handler '{0}' has [KubeJob] but does not implement IKubeJob<TPayload>",
        category: "KubeJob.Generators",
        defaultSeverity: DiagnosticSeverity.Error,
        isEnabledByDefault: true);

    private static readonly DiagnosticDescriptor DuplicateGeneratedProperty = new(
        id: "KJGEN002",
        title: "Duplicate generated job property",
        messageFormat: "Namespace '{0}' contains multiple KubeJob handlers that generate Jobs.{1}",
        category: "KubeJob.Generators",
        defaultSeverity: DiagnosticSeverity.Error,
        isEnabledByDefault: true);

    public void Initialize(IncrementalGeneratorInitializationContext context)
    {
        var candidates = context.SyntaxProvider.ForAttributeWithMetadataName(
                AttributeMetadataName,
                static (node, _) => node is ClassDeclarationSyntax,
                static (attributeContext, _) => CreateCandidate(attributeContext))
            .Where(static candidate => candidate is not null)
            .Collect();

        context.RegisterSourceOutput(candidates, static (sourceContext, collected) =>
            Generate(sourceContext, collected));
    }

    private static Candidate? CreateCandidate(GeneratorAttributeSyntaxContext context)
    {
        if (context.TargetSymbol is not INamedTypeSymbol handlerType)
        {
            return null;
        }

        var attribute = context.Attributes.FirstOrDefault();
        if (attribute is null
            || attribute.ConstructorArguments.Length == 0
            || attribute.ConstructorArguments[0].Value is not string jobKey
            || string.IsNullOrWhiteSpace(jobKey))
        {
            return null;
        }

        var payloadInterface = handlerType.AllInterfaces.FirstOrDefault(interfaceType =>
            interfaceType.OriginalDefinition.ToDisplayString() == GenericJobMetadataName);

        var className = handlerType.Name;
        var propertyName = CreatePropertyName(className);
        var namespaceName = handlerType.ContainingNamespace.IsGlobalNamespace
            ? string.Empty
            : handlerType.ContainingNamespace.ToDisplayString();
        var location = handlerType.Locations.FirstOrDefault();

        return new Candidate(
            namespaceName,
            propertyName,
            jobKey,
            payloadInterface?.TypeArguments[0],
            className,
            location);
    }

    private static void Generate(
        SourceProductionContext context,
        ImmutableArray<Candidate?> collected)
    {
        var candidates = collected
            .Where(static candidate => candidate is not null)
            .Select(static candidate => candidate!)
            .ToArray();

        foreach (var candidate in candidates.Where(static candidate => candidate.PayloadType is null))
        {
            context.ReportDiagnostic(Diagnostic.Create(
                MissingPayloadContract,
                candidate.Location,
                candidate.HandlerName));
        }

        foreach (var namespaceGroup in candidates
                     .Where(static candidate => candidate.PayloadType is not null)
                     .GroupBy(static candidate => candidate.NamespaceName, StringComparer.Ordinal))
        {
            var duplicates = namespaceGroup
                .GroupBy(static candidate => candidate.PropertyName, StringComparer.Ordinal)
                .Where(static group => group.Count() > 1)
                .ToArray();

            var duplicateNames = new HashSet<string>(
                duplicates.Select(static group => group.Key),
                StringComparer.Ordinal);
            foreach (var duplicate in duplicates.SelectMany(static group => group))
            {
                context.ReportDiagnostic(Diagnostic.Create(
                    DuplicateGeneratedProperty,
                    duplicate.Location,
                    namespaceGroup.Key,
                    duplicate.PropertyName));
            }

            var valid = namespaceGroup
                .Where(candidate => !duplicateNames.Contains(candidate.PropertyName))
                .OrderBy(static candidate => candidate.PropertyName, StringComparer.Ordinal)
                .ToArray();
            if (valid.Length == 0)
            {
                continue;
            }

            var source = Render(namespaceGroup.Key, valid);
            var hintName = string.IsNullOrEmpty(namespaceGroup.Key)
                ? "KubeJob.Jobs.Global.g.cs"
                : $"KubeJob.Jobs.{StableHint(namespaceGroup.Key)}.g.cs";
            context.AddSource(hintName, SourceText.From(source, Encoding.UTF8));
        }
    }

    private static string Render(string namespaceName, IReadOnlyList<Candidate> candidates)
    {
        var builder = new StringBuilder();
        builder.AppendLine("// <auto-generated />");
        builder.AppendLine("#nullable enable");
        if (!string.IsNullOrEmpty(namespaceName))
        {
            builder.Append("namespace ").Append(namespaceName).AppendLine(";");
            builder.AppendLine();
        }

        builder.AppendLine("public static partial class Jobs");
        builder.AppendLine("{");
        foreach (var candidate in candidates)
        {
            var payloadType = candidate.PayloadType!.ToDisplayString(SymbolDisplayFormat.FullyQualifiedFormat);
            var literal = SymbolDisplay.FormatLiteral(candidate.JobKey, quote: true);
            builder.Append("    public static global::KubeJob.Core.Jobs.JobKey<")
                .Append(payloadType)
                .Append("> ")
                .Append(candidate.PropertyName)
                .Append(" { get; } = new(")
                .Append(literal)
                .AppendLine(");");
        }
        builder.AppendLine("}");
        return builder.ToString();
    }

    private static string CreatePropertyName(string className)
    {
        var value = className.EndsWith("Job", StringComparison.Ordinal) && className.Length > 3
            ? className.Substring(0, className.Length - 3)
            : className;

        var builder = new StringBuilder(value.Length + 1);
        if (value.Length == 0 || !SyntaxFacts.IsIdentifierStartCharacter(value[0]))
        {
            builder.Append('_');
        }

        foreach (var character in value)
        {
            builder.Append(SyntaxFacts.IsIdentifierPartCharacter(character) ? character : '_');
        }

        var identifier = builder.ToString();
        return SyntaxFacts.GetKeywordKind(identifier) == SyntaxKind.None
            ? identifier
            : "_" + identifier;
    }

    private static string StableHint(string value)
    {
        unchecked
        {
            uint hash = 2166136261;
            foreach (var character in value)
            {
                hash ^= character;
                hash *= 16777619;
            }
            return hash.ToString("x8");
        }
    }

    private sealed class Candidate
    {
        public Candidate(
            string namespaceName,
            string propertyName,
            string jobKey,
            ITypeSymbol? payloadType,
            string handlerName,
            Location? location)
        {
            NamespaceName = namespaceName;
            PropertyName = propertyName;
            JobKey = jobKey;
            PayloadType = payloadType;
            HandlerName = handlerName;
            Location = location;
        }

        public string NamespaceName { get; }
        public string PropertyName { get; }
        public string JobKey { get; }
        public ITypeSymbol? PayloadType { get; }
        public string HandlerName { get; }
        public Location? Location { get; }
    }
}
