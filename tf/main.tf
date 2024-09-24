terraform {
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~> 4.0"
    }
  }
}

provider "google" {
  project = var.project_id
  region  = "us-central1"
}

resource "google_sql_database_instance" "postgres_instance" {
  name             = var.db_identifier
  database_version = "POSTGRES_16"
  region           = "us-central1"

  settings {
    tier = "db-custom-1-3840"
    
    availability_type = "ZONAL"
    
    ip_configuration {
      ipv4_enabled = true
      authorized_networks {
        name  = "mac"
        value = "69.157.17.251/32"
      }
    }
    
    backup_configuration {
      enabled                        = true
      start_time                     = "08:00"
      point_in_time_recovery_enabled = false
      transaction_log_retention_days = 7
      
      backup_retention_settings {
        retained_backups = 7
        retention_unit   = "COUNT"
      }
    }
    
    maintenance_window {
      day  = 7
      hour = 4
    }
    
    insights_config {
      query_insights_enabled  = true
      query_plans_per_minute  = 5
      query_string_length     = 2048
      record_application_tags = true
      record_client_address   = true
    }
    
    database_flags {
      name  = "max_connections"
      value = "100"
    }
  }

  deletion_protection = false

  labels = {
    project = "etl"
  }
}

resource "google_sql_database" "database" {
  name     = var.db_name
  instance = google_sql_database_instance.postgres_instance.name
}

resource "google_sql_user" "users" {
  name     = var.db_username
  instance = google_sql_database_instance.postgres_instance.name
  password = var.db_password
}