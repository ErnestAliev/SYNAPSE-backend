function createScopeContextService({
  toTrimmedString,
  toProfile,
  entityTypes,
}) {
  const AGENT_CHAT_SCOPE_TYPES = new Set(['collection', 'project']);
  const AGENT_CHAT_ENTITY_TYPES = new Set(Array.isArray(entityTypes) ? entityTypes : []);

  function normalizeScope(rawScope) {
    const scope = toProfile(rawScope);
    const scopeType = toTrimmedString(scope.type, 24).toLowerCase();

    if (!AGENT_CHAT_SCOPE_TYPES.has(scopeType)) {
      throw Object.assign(new Error('Invalid scope type'), { status: 400 });
    }

    if (scopeType === 'collection') {
      const entityType = toTrimmedString(scope.entityType, 24).toLowerCase();
      if (!AGENT_CHAT_ENTITY_TYPES.has(entityType)) {
        throw Object.assign(new Error('Invalid collection scope type'), { status: 400 });
      }
      return {
        type: 'collection',
        entityType,
        projectId: '',
        scopeKey: `collection:${entityType}`,
      };
    }

    const projectId = toTrimmedString(scope.projectId, 80);
    if (!projectId) {
      throw Object.assign(new Error('projectId is required for project scope'), { status: 400 });
    }

    return {
      type: 'project',
      entityType: '',
      projectId,
      // Must match frontend scope key format in AgentChatDock.
      scopeKey: `project-canvas:${projectId}`,
    };
  }

  function buildScopeKeyCandidates(scope) {
    if (!scope || typeof scope !== 'object') return [];
    if (scope.type === 'project') {
      const projectId = toTrimmedString(scope.projectId, 80);
      if (!projectId) return [];
      return Array.from(new Set([
        scope.scopeKey,
        `project:${projectId}`,
      ]));
    }
    return scope.scopeKey ? [scope.scopeKey] : [];
  }

  return {
    normalizeScope,
    buildScopeKeyCandidates,
  };
}

module.exports = {
  createScopeContextService,
};
