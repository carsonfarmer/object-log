mock_provider "aws" {
  mock_data "aws_subnets" {
    defaults = { ids = ["subnet-test"] }
  }
  mock_data "aws_subnet" {
    defaults = { vpc_id = "vpc-test" }
  }
  mock_data "aws_route53_zone" {
    defaults = { name = "example.com.", private_zone = false }
  }
  mock_resource "aws_cognito_user_pool" {
    defaults = { id = "us-west-2_test", endpoint = "cognito-idp.us-west-2.amazonaws.com/us-west-2_test" }
  }
  mock_resource "aws_eip" {
    defaults = { public_ip = "198.51.100.42" }
  }
}

variables {
  aws_profile   = "unused-mock"
  aws_region    = "us-west-2"
  run_id        = "host-test"
  bucket_name   = "object-log-host-test"
  object_prefix = "qualification/host-test"
  # Only the file checksum is used by the mocked provider; this is not an app bundle.
  host_artifact_path = "./maintenance.py"
  host_repositories = {
    "team/a.git" = { log_id = "a", format = "sha1" }
  }
}

run "s3_only" {
  command = plan
  assert {
    condition = (
      length(aws_instance.host) == 0 && length(aws_cognito_user_pool.git) == 0 &&
      length(aws_eip.host) == 0 && length(aws_eip_association.host) == 0 &&
      length(data.aws_route53_zone.host) == 0 && length(aws_route53_record.host) == 0
    )
    error_message = "Existing S3-only use must not create hosting resources."
  }
}

run "host" {
  command = apply
  variables {
    host_service              = true
    host_route53_zone_id      = "zone-test"
    host_name                 = "git.example.com"
    host_access_token_minutes = 5
    host_groups               = ["readers", "writers"]
    host_repositories = {
      "team/a.git" = { log_id = "a", format = "sha1", read_groups = ["readers"], write_groups = ["writers"] }
      "team/b.git" = { log_id = "b", format = "sha256" }
    }
  }
  assert {
    condition = (
      aws_instance.host[0].metadata_options[0].http_tokens == "required" &&
      aws_instance.host[0].metadata_options[0].http_put_response_hop_limit == 1 &&
      local.host_variables.wal_credential_mode == "instance-role" &&
      local.host_variables.git_auth_mode == "cognito"
    )
    error_message = "Hosting must require IMDSv2 and Cognito without static credentials."
  }
  assert {
    condition = (
      aws_cognito_user_pool_client.git[0].generate_secret == false &&
      aws_cognito_user_pool_client.git[0].access_token_validity == 5 &&
      aws_cognito_user_pool_client.git[0].allowed_oauth_flows == toset(["code"]) &&
      aws_cognito_user_pool_client.git[0].callback_urls == toset(["http://localhost:53119"]) &&
      aws_cognito_user_pool_client.maintenance[0].allowed_oauth_flows == toset(["client_credentials"]) &&
      aws_cognito_user_pool_client.maintenance[0].allowed_oauth_scopes == toset(["git/access", "git/maintenance"])
    )
    error_message = "Interactive and maintenance clients must have separate grants and scopes."
  }
  assert {
    condition = (
      jsondecode(aws_iam_role_policy.host[0].policy).Statement[1].Resource == "${aws_s3_bucket.qualification.arn}/qualification/host-test/git/*" &&
      jsondecode(aws_iam_role_policy.host[0].policy).Statement[4].Effect == "Deny" &&
      jsondecode(aws_iam_role_policy.host[0].policy).Statement[4].NotResource == aws_ssm_parameter.maintenance_secret[0].arn
    )
    error_message = "Host data access and Parameter Store access must stay within this run."
  }
  assert {
    condition     = !strcontains(local.host_bootstrap, aws_cognito_user_pool_client.maintenance[0].client_secret)
    error_message = "Client secrets must not enter EC2 user data."
  }
  assert {
    condition = (
      output.host_service_url == "https://git.example.com" &&
      aws_route53_record.host[0].records == toset(["198.51.100.42"]) &&
      !strcontains(local.host_bootstrap, "default_sni") &&
      !strcontains(local.host_bootstrap, "profile shortlived")
    )
    error_message = "Domain hosting must retain its DNS record and default Caddy certificate policy."
  }
}

run "host_public_ip" {
  command = apply
  variables {
    host_service = true
  }
  assert {
    condition = (
      length(data.aws_route53_zone.host) == 0 && length(aws_route53_record.host) == 0 &&
      aws_eip_association.host[0].allocation_id == aws_eip.host[0].id &&
      aws_eip_association.host[0].instance_id == aws_instance.host[0].id &&
      output.host_service_url == "https://198.51.100.42"
    )
    error_message = "Public-IP hosting must associate the allocated EIP without any Route53 resource."
  }
  assert {
    condition = (
      strcontains(local.host_bootstrap, "default_sni 198.51.100.42") &&
      strcontains(local.host_bootstrap, "198.51.100.42 {\n    tls {") &&
      strcontains(local.host_bootstrap, "issuer acme https://acme-v02.api.letsencrypt.org/directory") &&
      strcontains(local.host_bootstrap, "profile shortlived") &&
      jsondecode(base64decode(regex("printf '%s' '([^']+)' \\| base64 --decode > /etc/object-log-git/maintenance.json", local.host_bootstrap)[0])).service_url == output.host_service_url
    )
    error_message = "The rendered proxy and worker must use trusted public-IP HTTPS at the same address."
  }
}

run "hostname_without_zone" {
  command = plan
  variables {
    host_service = true
    host_name    = "git.example.com"
  }
  expect_failures = [aws_instance.host]
}

run "zone_without_hostname" {
  command = plan
  variables {
    host_service         = true
    host_route53_zone_id = "zone-test"
  }
  expect_failures = [aws_instance.host]
}
