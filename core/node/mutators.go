package node

// Registers the given handler when a compressed archive is fetched from a remote source.
func (n *NodeCore) RegisterDatasetHandler(f clientRequestHandler) {
	n.datasetHandlerLock.Lock()
	defer n.datasetHandlerLock.Unlock()
	n.datasetHandlerFunc = f
}

func (n *NodeCore) getDatasetHandler() clientRequestHandler {
	n.datasetHandlerLock.RLock()
	defer n.datasetHandlerLock.RUnlock()
	return n.datasetHandlerFunc
}

// Registers the given handler when external metadata is requested.
func (n *NodeCore) RegisterMetadataHandler(f clientRequestHandler) {
	n.metadataHandlerLock.Lock()
	defer n.metadataHandlerLock.Unlock()
	n.metadataHandlerFunc = f
}

func (n *NodeCore) getMetadataHandler() clientRequestHandler {
	n.metadataHandlerLock.RLock()
	defer n.metadataHandlerLock.RUnlock()
	return n.metadataHandlerFunc
}
