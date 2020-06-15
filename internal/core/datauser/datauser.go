package datauser

type DataUser struct {
	clientConfig *tls.Config
}

func NewDataUser(name string) (*DataUser, error) {
	if err := readConfig(); err != nil {
		panic(err)
	}

	pk := pkix.Name{
		Locality: []string{name}, // perhaps use another field instead of Locality?
	}

	cu, err := comm.NewCu(pk, viper.GetString("lohpi_ca_addr"))
	if err != nil {
		return nil, err
	}
}
