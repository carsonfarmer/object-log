variable "host_access_token_minutes" {
  description = "Interactive access-token lifetime; use five minutes for the live helper expiry test."
  type        = number
  default     = 60
  validation {
    condition = (
      var.host_access_token_minutes >= 5 && var.host_access_token_minutes <= 1440 &&
      floor(var.host_access_token_minutes) == var.host_access_token_minutes
    )
    error_message = "host_access_token_minutes must be an integer from 5 to 1440."
  }
}

data "aws_caller_identity" "host" {
  count = var.host_service ? 1 : 0
}

resource "aws_cognito_user_pool" "git" {
  count = var.host_service ? 1 : 0
  name  = "object-log-${var.run_id}"
  admin_create_user_config {
    allow_admin_create_user_only = true
  }
}

resource "aws_cognito_user_pool_domain" "git" {
  count                 = var.host_service ? 1 : 0
  domain                = "object-log-${var.run_id}-${data.aws_caller_identity.host[0].account_id}"
  user_pool_id          = aws_cognito_user_pool.git[0].id
  managed_login_version = 1
}

resource "aws_cognito_resource_server" "git" {
  count        = var.host_service ? 1 : 0
  identifier   = "git"
  name         = "Git service"
  user_pool_id = aws_cognito_user_pool.git[0].id
  scope {
    scope_name        = "access"
    scope_description = "Access subject to repository group permissions"
  }
  scope {
    scope_name        = "maintenance"
    scope_description = "Maintenance client administrative operations"
  }
}

resource "aws_cognito_user_pool_client" "git" {
  count                                = var.host_service ? 1 : 0
  name                                 = "git-credential-oauth"
  user_pool_id                         = aws_cognito_user_pool.git[0].id
  generate_secret                      = false
  allowed_oauth_flows_user_pool_client = true
  allowed_oauth_flows                  = ["code"]
  allowed_oauth_scopes                 = ["git/access"]
  callback_urls                        = ["http://localhost:53119"]
  default_redirect_uri                 = "http://localhost:53119"
  supported_identity_providers         = ["COGNITO"]
  prevent_user_existence_errors        = "ENABLED"
  enable_token_revocation              = true
  explicit_auth_flows                  = ["ALLOW_REFRESH_TOKEN_AUTH"]
  access_token_validity                = var.host_access_token_minutes
  refresh_token_validity               = 30
  token_validity_units {
    access_token  = "minutes"
    refresh_token = "days"
  }
  depends_on = [aws_cognito_resource_server.git]
}

resource "aws_cognito_user_group" "git" {
  for_each     = var.host_service ? var.host_groups : toset([])
  name         = each.value
  user_pool_id = aws_cognito_user_pool.git[0].id
}

resource "aws_cognito_user_pool_client" "maintenance" {
  count                                = var.host_service ? 1 : 0
  name                                 = "git-maintenance"
  user_pool_id                         = aws_cognito_user_pool.git[0].id
  generate_secret                      = true
  allowed_oauth_flows_user_pool_client = true
  allowed_oauth_flows                  = ["client_credentials"]
  allowed_oauth_scopes                 = ["git/access", "git/maintenance"]
  access_token_validity                = 60
  token_validity_units {
    access_token = "minutes"
  }
  depends_on = [aws_cognito_resource_server.git]
}

# Sensitive values remain in the local Terraform state and any saved plan.
# Only the parameter name enters EC2 user data; the worker fetches it at runtime.
resource "aws_ssm_parameter" "maintenance_secret" {
  count = var.host_service ? 1 : 0
  name  = "/object-log/${var.run_id}/maintenance-client-secret"
  type  = "SecureString"
  value = aws_cognito_user_pool_client.maintenance[0].client_secret
}

locals {
  cognito_login_origin = var.host_service ? "https://${aws_cognito_user_pool_domain.git[0].domain}.auth.${var.aws_region}.amazoncognito.com" : ""
}

output "host_cognito" {
  value = var.host_service ? {
    user_pool_id  = aws_cognito_user_pool.git[0].id
    issuer        = "https://${aws_cognito_user_pool.git[0].endpoint}"
    client_id     = aws_cognito_user_pool_client.git[0].id
    authorize_url = "${local.cognito_login_origin}/oauth2/authorize"
    token_url     = "${local.cognito_login_origin}/oauth2/token"
    scopes        = "git/access"
    redirect_url  = "http://localhost:53119"
  } : null
}
