variable "supabase_access_token" { type = string; sensitive = true }
variable "supabase_project_ref"  { type = string }
variable "supabase_db_password"  { type = string; sensitive = true }
variable "supabase_db_url"       { type = string; sensitive = true }
variable "kestra_image" {
  type    = string
  default = "kestra/kestra:latest-full"
}
