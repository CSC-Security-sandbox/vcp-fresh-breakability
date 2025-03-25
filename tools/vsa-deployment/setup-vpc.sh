#!/bin/bash


PROJECT_ID="<replace project id>"
REGION="us-east1"

#firewall allowed sources
SOURCE_RANGES="10.0.0.0/8,172.16.0.0/12,192.168.0.0/16,46.149.16.0/20,52.94.203.152/29,52.94.203.160/29,185.35.244.0/22,202.3.112.0/20,216.240.16.0/20,217.70.208.0/20"

for i in {1..5}; do
  VPC_NAME="my-vpc-$i"
  SUBNET_NAME="my-subnet-$i"
  FIREWALL_NAME="ingress-$VPC_NAME"

  echo "Creating VPC: $VPC_NAME in project $PROJECT_ID..."
  gcloud compute networks create "$VPC_NAME" \
    --subnet-mode=custom \
    --project="$PROJECT_ID"

  echo "Creating subnet: $SUBNET_NAME in $VPC_NAME..."
  gcloud compute networks subnets create "$SUBNET_NAME" \
    --network="$VPC_NAME" \
    --range="10.$i.0.0/23" \
    --region="$REGION" \
    --project="$PROJECT_ID"

  echo "Creating firewall rule: $FIREWALL_NAME for VPC: $VPC_NAME..."
  gcloud compute firewall-rules create "$FIREWALL_NAME" \
    --direction=INGRESS \
    --priority=1000 \
    --network="$VPC_NAME" \
    --allow tcp,udp,icmp \
    --source-ranges="$SOURCE_RANGES" \
    --project="$PROJECT_ID"

done

echo "All VPCs, subnets, and firewall rules created successfully in project $PROJECT_ID!"
