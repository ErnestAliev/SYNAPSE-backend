const {
  createAgentPrompts,
  cleanContextData,
} = require('./prompts/agent.prompts');
const { createProjectEnrichmentPrompts } = require('./prompts/project-enrichment.prompts');
const { createEntityProtectedPrompts } = require('./prompts/entity.protected.prompts');

function createAiPrompts(deps) {
  const agentPrompts = createAgentPrompts(deps);
  const projectEnrichmentPrompts = createProjectEnrichmentPrompts(deps);
  const entityProtectedPrompts = createEntityProtectedPrompts(deps);

  return {
    buildAgentContextData: agentPrompts.buildAgentContextData,
    buildAgentLlmContextData: agentPrompts.buildAgentLlmContextData,
    buildRouterPrompt: agentPrompts.buildRouterPrompt,
    normalizeDetectedRole: agentPrompts.normalizeDetectedRole,
    selectAgentRolesOnDemand: agentPrompts.selectAgentRolesOnDemand,
    evaluateAgentQuestionGate: agentPrompts.evaluateAgentQuestionGate,
    inspectAgentReplyQuestionGate: agentPrompts.inspectAgentReplyQuestionGate,
    buildAgentSystemPrompt: agentPrompts.buildAgentSystemPrompt,
    buildAgentUserPrompt: agentPrompts.buildAgentUserPrompt,
    buildProjectEnrichmentSystemPrompt: projectEnrichmentPrompts.buildProjectEnrichmentSystemPrompt,
    buildProjectEnrichmentUserPrompt: projectEnrichmentPrompts.buildProjectEnrichmentUserPrompt,
    buildProjectContextBuildSystemPrompt: projectEnrichmentPrompts.buildProjectContextBuildSystemPrompt,
    buildProjectContextBuildUserPrompt: projectEnrichmentPrompts.buildProjectContextBuildUserPrompt,
    buildEntityAnalyzerSystemPrompt: entityProtectedPrompts.buildEntityAnalyzerSystemPrompt,
    buildEntityAnalyzerUserPrompt: entityProtectedPrompts.buildEntityAnalyzerUserPrompt,
    buildEntityAnalysisReplyText: entityProtectedPrompts.buildEntityAnalysisReplyText,
  };
}

module.exports = {
  createAiPrompts,
  cleanContextData,
};
