variable "region" {
  type    = string
  default = "us-east-1"
}

variable "project_name" {
  type    = string
  default = "ceaser"
}

variable "instance_type" {
  description = "t4g.small (~$12/mo, 2GB RAM) is the sweet spot for ~100 users. Bump to t4g.medium if you see OOMs."
  type        = string
  default     = "t4g.small"
}

variable "root_volume_gb" {
  type    = number
  default = 20
}

variable "domain_name" {
  description = "Apex domain managed in Route53 (e.g. ceaser.app)"
  type        = string
}

variable "subdomain" {
  description = "Subdomain for the backend API"
  type        = string
  default     = "api"
}

variable "manage_dns_in_route53" {
  description = "If true, create an A record in Route53. Set false if DNS is managed elsewhere (Cloudflare, registrar, etc) — you'll add the A record manually post-apply."
  type        = bool
  default     = false
}

variable "create_route53_zone" {
  description = "Create a new Route53 hosted zone. Only used when manage_dns_in_route53=true."
  type        = bool
  default     = false
}

variable "ssh_public_key" {
  description = "Your SSH public key contents (cat ~/.ssh/id_ed25519.pub)"
  type        = string
}

variable "ssh_allowed_cidr" {
  description = "CIDR allowed to SSH. Lock to your IP (curl ifconfig.me)/32 once you are set up."
  type        = string
  default     = "0.0.0.0/0"
}

variable "git_repo_url" {
  description = "Git URL cloned on boot. For private repos embed a PAT: https://<token>@github.com/you/ceaser.git"
  type        = string
  sensitive   = true
}

variable "git_branch" {
  type    = string
  default = "main"
}

variable "env_vars" {
  description = "Backend env vars rendered into backend/.env on the instance. Put all secrets here (GEMINI_API_KEY, CLERK_*, NEO4J_*, ENCRYPTION_KEY, etc)."
  type        = map(string)
  sensitive   = true
  default     = {}
}

variable "deploy_lambda" {
  description = "Flip to true after the first ECR image push so terraform creates the Lambda function and event source mapping."
  type        = bool
  default     = false
}

variable "deploy_fargate" {
  description = "Flip to true after the first ECR image push so terraform creates the Fargate task definition."
  type        = bool
  default     = false
}
