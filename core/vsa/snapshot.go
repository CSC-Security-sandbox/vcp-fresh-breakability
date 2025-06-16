package vsa

import (
	"fmt"
	"strings"

	vsaerrors "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/errors"
	ontapRest "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/ontap-rest"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/nillable"
)

// CreateSnapshot creates a snapshot by calling the ONTAP REST Client
func (rc *OntapRestProvider) CreateSnapshot(params CreateSnapshotParams) (*SnapshotProviderResponse, error) {
	client := getOntapClientFunc(rc.ClientParams)
	snapshot, job, err := client.Storage().SnapshotCreate(&ontapRest.SnapshotCreateParams{
		VolumeUUID: params.VolumeUUID,
		Name:       params.Name,
		Comment:    nillable.ToPointer(params.Comment),
	})
	if err != nil {
		if !errors.IsNotFoundErr(err) {
			return nil, vsaerrors.NewVCPError(vsaerrors.ErrOntapRestAPIError, err)
		}
		return nil, vsaerrors.NewVCPError(vsaerrors.ErrResourceNotFound, errors.NewNotFoundErr("Volume", nil))
	}

	var uuid string
	if snapshot != nil && snapshot.UUID != nil {
		uuid = *snapshot.UUID
	} else {
		// Poll the job if it exists
		if job != nil {
			if err = client.Poll(job.JobUUID); err != nil {
				return nil, vsaerrors.NewVCPError(vsaerrors.ErrOntapRestAPIError, err)
			}
			uuid = job.ResourceUUID
		}
	}

	snapshot, err = client.Storage().SnapshotGet(&ontapRest.SnapshotGetParams{
		BaseParams: ontapRest.BaseParams{Fields: []string{
			"reclaimable_space",
			"size",
			"logical_size",
		}},
		UUID:       uuid,
		VolumeUUID: params.VolumeUUID,
	})
	if err != nil {
		return nil, vsaerrors.NewVCPError(vsaerrors.ErrOntapRestAPIError, err)
	}
	// Validate the Snapshot response to avoid nil pointer dereferences
	if snapshot == nil {
		return nil, vsaerrors.NewVCPError(vsaerrors.ErrOntapInconsistentResourceError, errors.NewBadRequestErr("invalid Snapshot create response from API: snapshot is nil"))
	}
	if snapshot.Name == nil || snapshot.UUID == nil {
		return nil, vsaerrors.NewVCPError(vsaerrors.ErrOntapInconsistentResourceError, errors.NewBadRequestErr("invalid Snapshot create response from API: missing required fields"))
	}

	// Return the created SVM
	return &SnapshotProviderResponse{
		ProviderResponse: ProviderResponse{
			Name:         *snapshot.Name,
			ExternalUUID: *snapshot.UUID,
		},
		SizeInBytes:        *snapshot.Size,
		LogicalSizeInBytes: *snapshot.LogicalSize,
	}, nil
}

// DeleteSnapshot deletes a snapshot by calling the ONTAP REST Client
func (rc *OntapRestProvider) DeleteSnapshot(snapshotUUID string, volumeUUID string) error {
	client := getOntapClientFunc(rc.ClientParams)
	err := client.Storage().SnapshotDelete(&ontapRest.SnapshotDeleteParams{
		UUID:       snapshotUUID,
		VolumeUUID: volumeUUID,
	})

	if err != nil {
		return vsaerrors.NewVCPError(vsaerrors.ErrOntapRestAPIError, err)
	}

	return nil
}

func (rc *OntapRestProvider) CreateSnapshotPolicy(sp *SnapshotPolicy) error {
	client := getOntapClientFunc(rc.ClientParams)
	if len(sp.Schedules) == 0 {
		return errors.New("must have at least one snapshot policy schedule when creating")
	}
	if len(sp.Schedules) > 4 {
		return errors.New("too many snapshot policy schedules specified")
	}

	schedules := make([]*ontapRest.SnapshotPolicySchedule, len(sp.Schedules))
	for i, schedule := range sp.Schedules {
		schedules[i] = &ontapRest.SnapshotPolicySchedule{
			Prefix:          schedule.SnapmirrorLabel,
			Count:           schedule.Count,
			SnapmirrorLabel: schedule.SnapmirrorLabel,
			Name:            schedule.Schedule.Name,
			Months:          schedule.Schedule.Months,
			DaysOfMonth:     schedule.Schedule.DaysOfMonth,
			DaysOfWeek:      schedule.Schedule.DaysOfWeek,
			Hours:           schedule.Schedule.Hours,
			Minutes:         schedule.Schedule.Minutes,
		}
	}
	for i := 0; i < len(schedules); i++ {
		schedules[i].Name = generateNameForSchedule(&Schedule{
			Months:      schedules[i].Months,
			DaysOfMonth: schedules[i].DaysOfMonth,
			DaysOfWeek:  schedules[i].DaysOfWeek,
			Hours:       schedules[i].Hours,
			Minutes:     schedules[i].Minutes,
		})
	}

	err := client.Storage().SnapshotPolicyCreate(&ontapRest.SnapshotPolicyCreateParams{
		Name:      &sp.Name,
		Comment:   &sp.Comment,
		Enabled:   &sp.IsEnabled,
		Schedules: schedules,
	})

	if err != nil {
		if strings.Contains(err.Error(), "not found") && strings.HasPrefix(err.Error(), "Schedule") {
			for _, schedule := range schedules {
				err := client.Cluster().ScheduleCreate(&ontapRest.ScheduleCreateParams{
					Name:        schedule.Name,
					Months:      schedule.Months,
					DaysOfMonth: schedule.DaysOfMonth,
					DaysOfWeek:  schedule.DaysOfWeek,
					Hours:       schedule.Hours,
					Minutes:     schedule.Minutes,
				})
				if err != nil {
					if !strings.Contains(err.Error(), "exists") &&
						!strings.Contains(err.Error(), "duplicate entry") {
						return err
					}
				}
			}

			return client.Storage().SnapshotPolicyCreate(&ontapRest.SnapshotPolicyCreateParams{
				Name:      &sp.Name,
				Comment:   &sp.Comment,
				Enabled:   &sp.IsEnabled,
				Schedules: schedules,
			})
		}

		if !strings.Contains(err.Error(), "exists") {
			return err
		}
	}
	return nil
}

func generateNameForSchedule(schedule *Schedule) string {
	// generateNameForSchedule returns a name generated for the specified schedule
	if len(schedule.DaysOfMonth) > 0 {
		daysOfMonth := strings.ReplaceAll(strings.Trim(fmt.Sprintf("%v", schedule.DaysOfMonth), "[]"), " ", "+")
		return fmt.Sprintf("monthly-on-day-%s-%s-%s", daysOfMonth, utils.GenerateMinutePartOfScheduleName(schedule.Minutes), utils.GenerateHourPartOfScheduleName(schedule.Hours))
	}
	if len(schedule.DaysOfWeek) > 0 {
		return fmt.Sprintf("weekly-on-%s-%s-%s", utils.GenerateWeekdayPartOfScheduleName(schedule.DaysOfWeek), utils.GenerateMinutePartOfScheduleName(schedule.Minutes), utils.GenerateHourPartOfScheduleName(schedule.Hours))
	}
	if len(schedule.Hours) > 0 {
		return fmt.Sprintf("daily-%s-%s", utils.GenerateMinutePartOfScheduleName(schedule.Minutes), utils.GenerateHourPartOfScheduleName(schedule.Hours))
	}
	return fmt.Sprintf("hourly-%s-hour", utils.GenerateMinutePartOfScheduleName(schedule.Minutes))
}
