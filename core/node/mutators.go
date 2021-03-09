package node

// Registers the given handler when a compressed archive is fetched from a remote source.
func (n *Node) RegisterDatasetHandler(f ExternalArchiveHandler) {
	n.datasetCallbackLock.Lock()
	defer n.datasetCallbackLock.Unlock()
	n.datasetCallback = f
}

func (n *Node) getDatasetCallback() ExternalArchiveHandler {
	n.datasetCallbackLock.RLock()
	defer n.datasetCallbackLock.RUnlock()
	return n.datasetCallback
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
