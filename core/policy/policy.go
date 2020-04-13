package policy

type SubjectPolicy struct {
	Subject string
	Node string
	Study string
	RequiredAttributes map[string]string
}

/*func NewSubjectPolicy(node, study string, attributes map[string][]string) *SubjectPolicy {
	return &SubjectPolicy{
		Node: node,
		Study: study,
		RequiredAttributes: attributes,
	}
}*/

//func (sp *SubjectPolicy) AddAttributes(attributes) 