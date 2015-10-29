package database

import (
	"encoding/json"
	"fmt"
	"time"

	"bosun.org/_third_party/github.com/garyburd/redigo/redis"
	"bosun.org/collect"
	"bosun.org/models"
	"bosun.org/opentsdb"
)

/*

incident:{id} -> json of incident
maxIncidentId -> counter. Increment to get next id.

*/

type IncidentDataAccess interface {
	GetIncident(id uint64) (*models.Incident, error)
	CreateIncident(ak models.AlertKey, start time.Time) (*models.Incident, error)
	UpdateIncident(id uint64, i *models.Incident) error
}

func (d *dataAccess) Incidents() IncidentDataAccess {
	return d
}
func incidentKey(id uint64) string {
	return fmt.Sprint("incident:%d", id)
}
func (d *dataAccess) GetIncident(id uint64) (*models.Incident, error) {
	defer collect.StartTimer("redis", opentsdb.TagSet{"op": "GetIncident"})()
	conn := d.GetConnection()
	defer conn.Close()
	raw, err := redis.Bytes(conn.Do("GET", incidentKey(id)))
	if err != nil {
		return nil, err
	}
	incident := &models.Incident{}
	if err = json.Unmarshal(raw, incident); err != nil {
		return nil, err
	}
	return incident, nil
}

func (d *dataAccess) CreateIncident(ak models.AlertKey, start time.Time) (*models.Incident, error) {
	defer collect.StartTimer("redis", opentsdb.TagSet{"op": "CreateIncident"})()
	conn := d.GetConnection()
	defer conn.Close()
	id, err := redis.Int64(conn.Do("INCR", "maxIncidentId"))
	if err != nil {
		return nil, err
	}
	incident := &models.Incident{
		Id:       uint64(id),
		Start:    time.Now(),
		AlertKey: ak,
	}
	err = saveIncident(incident.Id, incident, conn)
	if err != nil {
		return nil, err
	}
	return incident, nil
}

func saveIncident(id uint64, i *models.Incident, conn redis.Conn) error {
	raw, err := json.Marshal(i)
	if err != nil {
		return err
	}
	_, err = conn.Do("SET", incidentKey(id), raw)
	return err
}

func (d *dataAccess) UpdateIncident(id uint64, i *models.Incident) error {
	defer collect.StartTimer("redis", opentsdb.TagSet{"op": "UpdateIncident"})()
	conn := d.GetConnection()
	defer conn.Close()
	return saveIncident(id, i, conn)
}
