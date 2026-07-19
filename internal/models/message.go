package models

import "collabflow/internal/messaging"

// Message is an alias for messaging.Event to preserve backwards compatibility.
type Message = messaging.Event
