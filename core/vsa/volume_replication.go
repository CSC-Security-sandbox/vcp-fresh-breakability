package vsa

import (
	ontaprest "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/ontap-rest"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/errors"
)

const (
	VolumeReplicationSchedule10Minutely = "10minutely"
	VolumeReplicationScheduleHourly     = "hourly"
	VolumeReplicationScheduleDaily      = "daily"
)

func (rc *OntapRestProvider) CreateVolumeReplicationSchedule(schedule string) (err error) {
	var cronSchedule *ontaprest.Schedule
	client := getOntapClientFunc(rc.ClientParams)
	if err := client.Cluster().ScheduleCollectionGet(&ontaprest.ScheduleCollectionGetParams{
		Name: schedule,
	}, func(schedules []*ontaprest.Schedule) error {
		if len(schedules) == 1 {
			cronSchedule = schedules[0]
		}
		return nil
	}); err != nil && !errors.IsNotFoundErrForObjectType(err, "Cron schedule") {
		return err
	}

	if cronSchedule != nil {
		return nil
	}
	switch schedule {
	case VolumeReplicationSchedule10Minutely:
		err = client.Cluster().ScheduleCreate(&ontaprest.ScheduleCreateParams{
			Name:        schedule,
			Months:      nil,
			DaysOfMonth: nil,
			DaysOfWeek:  nil,
			Hours:       nil,
			Minutes:     []int{0, 10, 20, 30, 40, 50},
		})
	case VolumeReplicationScheduleHourly:
		err = client.Cluster().ScheduleCreate(&ontaprest.ScheduleCreateParams{
			Name:        schedule,
			Months:      nil,
			DaysOfMonth: nil,
			DaysOfWeek:  nil,
			Hours:       nil,
			Minutes:     []int{5},
		})
	case VolumeReplicationScheduleDaily:
		err = client.Cluster().ScheduleCreate(&ontaprest.ScheduleCreateParams{
			Name:        schedule,
			Months:      nil,
			DaysOfMonth: nil,
			DaysOfWeek:  nil,
			Hours:       []int{0},
			Minutes:     []int{5},
		})
	default:
		err = errors.NewUserInputValidationErr("Unknown replication schedule")
	}
	if err != nil {
		return err
	}
	return nil
}
