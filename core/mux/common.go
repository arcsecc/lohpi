package mux

func (m *Mux) nodeExists(node string) bool {
	if _, ok := m.nodes[node]; ok {
		return true
	}
	return false
}