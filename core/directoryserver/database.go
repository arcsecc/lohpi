package directoryserver

import (
	"context"
	"errors"
	"github.com/jackc/pgx/v4/pgxpool"
	log "github.com/sirupsen/logrus"
)

var (
	nilEmptyId               = errors.New("Empty instance identifier")
	nilPgxPool               = errors.New("Database connection pool is nil")
	errSetProjectDescription = errors.New("Could not set project description")
	errGetProjectDescription = errors.New("Could not gset project description")
)

var dbLogFields = log.Fields{
	"package": "core/directoryserver",
	"action":  "database client",
}

type datasetDescriptionManagerUnit struct {
	pool                           *pgxpool.Pool
	directoryServerSchema          string
	datasetProjectDescriptionTable string
}

// Main entry point for initializing the database schema and its tables on Microsoft Azure
func newDatasetDescriptionManagerUnit(id string, pool *pgxpool.Pool) (*datasetDescriptionManagerUnit, error) {
	if id == "" {
		return nil, nilEmptyId
	}

	if pool == nil {
		return nil, nilPgxPool
	}

	return &datasetDescriptionManagerUnit{
		pool:                           pool,
		directoryServerSchema:          id + "_schema",
		datasetProjectDescriptionTable: id + "_dataset_description_table",
	}, nil
}

func (d *datasetDescriptionManagerUnit) dbSetProjectDescription(datasetId string, projectDescription string) error {
	q := `INSERT INTO ` + d.directoryServerSchema + `.` + d.datasetProjectDescriptionTable + ` 
	(dataset_id, description) VALUES ($1, $2)
	ON CONFLICT (dataset_id)
	DO
		UPDATE SET description = $2;`

	_, err := d.pool.Exec(context.Background(), q, datasetId, projectDescription)
	if err != nil {
		log.WithFields(dbLogFields).Error(err.Error())
		return errSetProjectDescription
	}

	return nil
}

func (d *datasetDescriptionManagerUnit) dbGetProjectDescription(datasetId string) (string, error) {
	q := `SELECT description FROM ` + d.directoryServerSchema + `.` + d.datasetProjectDescriptionTable + ` WHERE dataset_id = $1;`

	var projectDescription string

	if err := d.pool.QueryRow(context.Background(), q, datasetId).Scan(&projectDescription); err != nil {
		log.WithFields(dbLogFields).Error(err.Error())
		return "", errGetProjectDescription
	}

	return projectDescription, nil
}
