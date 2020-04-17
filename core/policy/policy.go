package policy

type SubjectPolicy struct {
	Subject string
	Node string
	Study string
	RequiredAttributes map[string]string
}