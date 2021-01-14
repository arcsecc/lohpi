package node

// Registers the given handler when a compressed archive is fetched from a remote source.
func (n *Node) RegisterArchiveHandler(f ExternalArchiveHandler) {
	n.archiveCallbackLock.Lock()
	defer n.archiveCallbackLock.Unlock()
	n.archiveCallback = f
}

func (n *Node) getArchiveCallback() ExternalArchiveHandler {
	n.archiveCallbackLock.RLock()
	defer n.archiveCallbackLock.RUnlock()
	return n.archiveCallback
}

// Registers the given callback whenever a new dataset from an external source is requested.
func (n *Node) RegisterDatasetIdentifiersHandler(f ExernalDatasetIdentifiersHandler) {
	n.datasetIdentifiersHandlerLock.Lock()
	defer n.datasetIdentifiersHandlerLock.Unlock()
	n.datasetIdentifiersHandler = f
}

func (n *Node) getDatasetIdentifiersHandler() ExernalDatasetIdentifiersHandler {
	n.datasetIdentifiersHandlerLock.RLock()
	defer n.datasetIdentifiersHandlerLock.RUnlock()
	return n.datasetIdentifiersHandler
}
// Registers the given handler when an identifier is to be checked that it exists or not
func (n *Node) RegisterIdentifierExistsHandler(f ExternalIdentifierExistsHandler) {
	n.identifierExistsHandlerLock.Lock()
	defer n.identifierExistsHandlerLock.Unlock()
	n.identifierExistsHandler = f
}

func (n *Node) getIdentifierExistsHandler() ExternalIdentifierExistsHandler {
	n.identifierExistsHandlerLock.RLock()
	defer n.identifierExistsHandlerLock.RUnlock()
	return n.identifierExistsHandler
}

// Registers the given handler when external metadata is requested.
func (n *Node) RegsiterMetadataHandler(f ExternalMetadataHandler) {
	n.externalMetadataHandlerLock.Lock()
	defer n.externalMetadataHandlerLock.Unlock()
	n.externalMetadataHandler = f
}

func (n *Node) getExternalMetadataHandler() ExternalMetadataHandler {
	n.externalMetadataHandlerLock.RLock()
	defer n.externalMetadataHandlerLock.RUnlock()
	return n.externalMetadataHandler
}
