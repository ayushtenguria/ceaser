output "public_ip" {
  value = aws_eip.app.public_ip
}

output "api_url" {
  value = "https://${local.api_fqdn}"
}

output "ssh_command" {
  value = "ssh ubuntu@${aws_eip.app.public_ip}"
}

output "s3_uploads_bucket" {
  value = aws_s3_bucket.uploads.bucket
}

output "sqs_queue_url" {
  value = aws_sqs_queue.excel_processing.url
}

output "ecr_excel_processor_url" {
  value = aws_ecr_repository.excel_processor.repository_url
}

output "hmac_shared_secret" {
  value     = random_password.hmac_secret.result
  sensitive = true
}

output "lambda_function_name" {
  value = try(aws_lambda_function.excel_processor[0].function_name, null)
}

output "route53_nameservers" {
  description = "Set these at your registrar if create_route53_zone=true"
  value       = var.manage_dns_in_route53 && var.create_route53_zone ? aws_route53_zone.main[0].name_servers : null
}

output "dns_instructions" {
  description = "If manage_dns_in_route53=false, add this A record manually at your DNS provider"
  value       = var.manage_dns_in_route53 ? "DNS managed by Terraform" : "Add an A record: ${local.api_fqdn}  →  ${aws_eip.app.public_ip}  (TTL 300)"
}
