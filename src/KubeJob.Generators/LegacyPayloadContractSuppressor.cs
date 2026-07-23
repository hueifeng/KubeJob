using System.Collections.Immutable;
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.Diagnostics;

namespace KubeJob.Generators;

/// <summary>
/// The migration release must continue compiling handlers that implement the
/// legacy non-generic IKubeJob contract. Such handlers simply do not receive a
/// generated typed key until they adopt IKubeJob&lt;TPayload&gt;.
/// </summary>
[DiagnosticAnalyzer(LanguageNames.CSharp)]
public sealed class LegacyPayloadContractSuppressor : DiagnosticSuppressor
{
    private static readonly SuppressionDescriptor LegacyHandlerSuppression = new(
        id: "KJGENSPR001",
        suppressedDiagnosticId: "KJGEN001",
        justification: "Legacy non-generic KubeJob handlers remain supported during the V2 migration window.");

    public override ImmutableArray<SuppressionDescriptor> SupportedSuppressions =>
        ImmutableArray.Create(LegacyHandlerSuppression);

    public override void ReportSuppressions(SuppressionAnalysisContext context)
    {
        foreach (var diagnostic in context.ReportedDiagnostics)
        {
            if (diagnostic.Id == "KJGEN001")
            {
                context.ReportSuppression(Suppression.Create(
                    LegacyHandlerSuppression,
                    diagnostic));
            }
        }
    }
}
