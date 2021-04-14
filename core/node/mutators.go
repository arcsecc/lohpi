package node

// Registers the given handler when a compressed archive is fetched from a remote source.
func (n *NodeCore) SetDatasetHandler(f externalArchiveHandler) {
	n.datasetCallbackLock.Lock()
	defer n.datasetCallbackLock.Unlock()
	n.datasetCallback = f
}

func (n *NodeCore) getDatasetCallback() externalArchiveHandler {
	n.datasetCallbackLock.RLock()
	defer n.datasetCallbackLock.RUnlock()
	return n.datasetCallback
}

// Registers the given handler when external metadata is requested.
func (n *NodeCore) SetMetadataHandler(f externalMetadataHandler) {
	n.externalMetadataCallbackLock.Lock()
	defer n.externalMetadataCallbackLock.Unlock()
	n.externalMetadataCallback = f
}

func (n *NodeCore) getExternalMetadataHandler() externalMetadataHandler {
	n.externalMetadataCallbackLock.RLock()
	defer n.externalMetadataCallbackLock.RUnlock()
	return n.externalMetadataCallback
}
