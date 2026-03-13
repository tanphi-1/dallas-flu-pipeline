output "dchhs_bucket" { value = supabase_storage_bucket.dchhs_pdfs.name }
output "dshs_bucket"  { value = supabase_storage_bucket.dshs_pdfs.name }
output "kestra_ui"    { value = "http://localhost:8080" }
