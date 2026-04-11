data "aws_route53_zone" "main" {
  count        = var.manage_dns_in_route53 && !var.create_route53_zone ? 1 : 0
  name         = var.domain_name
  private_zone = false
}

resource "aws_route53_zone" "main" {
  count = var.manage_dns_in_route53 && var.create_route53_zone ? 1 : 0
  name  = var.domain_name
}

locals {
  zone_id = var.manage_dns_in_route53 ? (
    var.create_route53_zone ? aws_route53_zone.main[0].zone_id : data.aws_route53_zone.main[0].zone_id
  ) : null
}

resource "aws_route53_record" "api" {
  count   = var.manage_dns_in_route53 ? 1 : 0
  zone_id = local.zone_id
  name    = local.api_fqdn
  type    = "A"
  ttl     = 300
  records = [aws_eip.app.public_ip]
}
