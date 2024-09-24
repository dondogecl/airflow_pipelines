variable "project_id" {
    description = "Name of the target GCP project"
    type        = string
}

variable "db_name" {
    description = "Database name"
    type        = string
}

variable "db_username" {
    description = "Database username"
    type        = string
}

variable "db_password" {
    description = "Database password"
    type        = string
    sensitive   = true
}

variable "db_identifier" {
    description = "Database instance identifier"
    type        = string

}