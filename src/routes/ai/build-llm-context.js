function createBuildLlmContext({
  toTrimmedString,
  toProfile,
  aiPrompts,
}) {
  function summarizePreviewEntities(entities) {
    return (Array.isArray(entities) ? entities : []).map((item) => {
      const entity = toProfile(item);
      const metadata = toProfile(entity.ai_metadata);
      const description = toTrimmedString(metadata.description || entity.description, 2400);
      const fieldCounts = {};
      let fieldsItemsTotal = 0;

      for (const [key, rawValue] of Object.entries(metadata)) {
        if (!Array.isArray(rawValue)) continue;
        const count = rawValue
          .map((value) => toTrimmedString(value, 240))
          .filter(Boolean)
          .length;
        if (!count) continue;
        fieldCounts[key] = count;
        fieldsItemsTotal += count;
      }

      return {
        id: toTrimmedString(entity.id || entity._id, 120),
        type: toTrimmedString(entity.type, 40),
        name: toTrimmedString(entity.name, 160) || '(без названия)',
        description,
        descriptionLength: description.length,
        fieldsItemsTotal,
        fieldCounts,
        updatedAt: toTrimmedString(entity.updatedAt, 80),
      };
    });
  }

  function buildRequestBodySize(requestBody) {
    if (!requestBody || typeof requestBody !== 'object') {
      return {
        chars: 0,
        bytes: 0,
      };
    }
    const serialized = JSON.stringify(requestBody);
    return {
      chars: serialized.length,
      bytes: Buffer.byteLength(serialized, 'utf8'),
    };
  }

  function mapRoleOnDemandToLegacyRole(roleKey) {
    const normalized = toTrimmedString(roleKey, 64);
    if (!normalized) return 'default';
    if (['financial_analyst', 'risk_analyst'].includes(normalized)) return 'investor';
    if (
      ['strategist', 'tactician_7_30', 'prioritizer', 'hidden_potential_hunter', 'illusion_breaker', 'negotiator']
        .includes(normalized)
    ) return 'strategist';
    if (['operations_analyst', 'change_archivist'].includes(normalized)) return 'hr';
    return 'default';
  }

  function resolveCompatibleDetectedRole(roleSelection, roleHint) {
    const hinted = aiPrompts.normalizeDetectedRole(toTrimmedString(roleHint, 24) || 'default');
    if (hinted && hinted !== 'default') return hinted;
    const firstSelectedKey = toTrimmedString(roleSelection?.selectedRoles?.[0]?.key, 64);
    return mapRoleOnDemandToLegacyRole(firstSelectedKey);
  }

  function buildAgentLlmContext({
    scopeContext,
    history,
    attachments,
    message,
  }) {
    return aiPrompts.buildAgentLlmContextData({
      scopeContext,
      history,
      attachments,
      message,
    });
  }

  return {
    summarizePreviewEntities,
    buildRequestBodySize,
    resolveCompatibleDetectedRole,
    buildAgentLlmContext,
  };
}

module.exports = {
  createBuildLlmContext,
};
