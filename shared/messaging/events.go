package messaging

import pb "ride-sharing/shared/proto/trip"

const (
	FindAvailableDriversEvent = "find_available_drivers"
)

type TripEventData struct {
	Trip *pb.Trip `json:"trip"`
}
