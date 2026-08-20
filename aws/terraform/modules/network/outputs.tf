output "vpc_id" {
  value = aws_vpc.this.id
}

output "public_subnet_ids" {
  value = aws_subnet.public[*].id
}

output "private_subnet_ids" {
  value = aws_subnet.private[*].id
}

output "nlb_sg_id" {
  value = aws_security_group.nlb.id
}

output "yarp_sg_id" {
  value = aws_security_group.yarp.id
}

output "app_sg_id" {
  value = aws_security_group.app.id
}

output "data_sg_id" {
  value = aws_security_group.data.id
}

output "efs_sg_id" {
  value = aws_security_group.efs.id
}

output "vpclink_sg_id" {
  value = aws_security_group.vpclink.id
}

output "db_sg_id" {
  value = aws_security_group.db.id
}
