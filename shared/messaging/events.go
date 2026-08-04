package messaging

import pb "ride-sharing/shared/proto/trip"

const (
	FindAvailableDriversEvent = "find_available_drivers"
	DriverCmdTripRequestQueue = "driver_cmd_trip_request"
)

type TripEventData struct {
	Trip *pb.Trip `json:"trip"`
}
