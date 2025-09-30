# 9. Hybrid Replications - Onprem Migration and Bi-Directional Snapmirror

Date: 2025-08-22

## Status

Pending

## Context

This document presents the high-level design approach for Onprem Migration and Bi-Directional Snapmirror for the VSA architecture.

## Requirements in Scope

- SnapMirror based migration from CVO and On-prem
- Bi-directional SnapMirror between GCNV Flex Block and CVO/On-prem
- Support for both Block & Files

## Investigation/Discussion Items

### 1. LUN Mapping Between Onprem Ontap and GCNV Block

Currently we support 1 Volume - 1 LUN model in GCNV. If onprem/CVO volume has more than 1 LUN per Volume, snapmirror will replicate multiple LUNs on the same volume.

#### Option 1: Migration with Multiple LUN Support

**Migration:**
- Allow customers to migrate volumes with multiple LUNs to GCNV
- Volume update workflow (to resize) will be adjusted to skip resizing LUNs if > 1 LUN is found in the volume
- Once the migration is finalized (i.e., replication is deleted), VCP will split all the LUNs (lun move) to separate volumes with 1 LUN each
- From this point onwards, these volumes will behave like normal GCNV volumes

**Bi-Directional Snapmirror:**
- Allow customers to migrate volumes with multiple LUNs to GCNV
- LUN resize and igroup management will be allowed via Expert mode
- Volume update workflow (to resize) will be adjusted to skip resizing LUNs if > 1 LUN is found in the volume
- When customers reverse the replication, the volume (with multiple LUNs) on GCNV will become the primary
- When customer finally deletes the replication, we can split into multiple volumes

#### Option 2: Abort on Multiple LUN Detection

- There is no way to identify the number of LUNs in a volume on external Ontap
- GCNV (or target in snapmirror) will only know about the number of LUNs in the volume once the baseline transfer starts
- For both Migration and Bi-Directional Snapmirror, VCP can monitor to check how many LUNs are getting replicated
- If > 1 LUN found, we can abort the transfer and alert the user via Error messages in the replication status
- To recover, customer can delete this replication, split LUNs into individual volumes and then re-initiate the migration/Bi-Directional Snapmirror

### 2. 1-1 SVM Peering

In VSA, we are currently following 1 Cluster - 1 SVM model. As per the security concern raised by Snapmirror team, it is ideal to disallow fan-in into GCNV SVMs from external clusters.

The plan is to enforce this via VCP to only allow 1-1 SVM peering between onprem and GCNV. This will require multiple SVMs to be created in the VSA cluster.

This poses certain challenges:

**Provisioning of IP ranges:**
- Currently each SVM requires 5 IP addresses
- With the current model, the only way to expand subnet is to add secondary ranges to the existing subnet

**Throughput (shared QOS):**
- Max throughput of each HA pair is around 1.8 GBps
- How will the shared QOS policy behave if we add > 1 SVM on the same cluster

#### Option 1: 1 Cluster Multiple SVMs
- Each SVM in the pool is peered with a unique onprem SVM

#### Option 2: Maintain 1 Cluster 1 SVM Model
- Stick to the 1 Cluster 1 SVM model of GCNV
- For each onprem SVM, customer create a separate pool

### 3. RBAC

By default in ONTAP, if two clusters are peered, then cluster admin can perform most of the actions on peered cluster via cross cluster APIs.

Currently in SDE, we follow a very restrictive RBAC role which only allows selective operations from the external cluster, e.g.:
- Cluster peering
- Vserver peering
- Snapmirror operations - on selective volumes only in case of Bi-Directional Snapmirror (ONTAPPM-109185 - Ability to restrict snapmirror from external peers on a per volume level - Committed to Project)

Since VSA is single tenancy, should we still follow the same restrictive RBAC? If we do not follow this, customers could get access to restricted operations.

## Decision

### 1. LUN Mapping Between Onprem & GCNV Block

**For Preview, the decision is to go with Option 2:**
- If more than one LUN is found, the transfer will be aborted
- The user will be alerted via error messages in the replication status
- Support for multiple LUNs will be added post GA

### 2. 1-1 SVM Peering

**Customers can only peer one on-prem SVM by default with GCNV:**

- For VSA, the GCNV pool will have only one SVM, and we do not currently support multiple SVMs
- If a customer tries to peer a second SVM, this will be blocked at the VCP layer by default
- If a customer wants to migrate from multiple on-prem SVMs, resulting in fan-in to the same GCNV SVM, they can be allow-listed. This will enable multiple on-prem SVMs to be peered with one SVM.
- Customers need to know beforehand that all on-prem SVMs from one cluster will land on a single SVM on the GCNV side if they plan to migrate multiple SVMs
- This block is only temporary. Once the actual ONTAP fix to support full-path RBAC is available, these issues will be resolved, and the allow-listing part can be disabled

## Pending

### Multiple SVMs in VSA

To support 1 Cluster multiple SVMs in VSA, it needs detailed discussion on:
- QOS policy
- Provisioning of IP ranges

A follow-up meeting will be scheduled for this.

## Consequences

### Positive Consequences

1. **Security**: Prevents unauthorized access through restrictive SVM peering
2. **Simplicity**: Maintains current 1-1 SVM model for VSA
3. **Control**: Allows controlled exceptions through allow-listing
4. **Future-Proof**: Temporary restrictions until ONTAP RBAC improvements

### Negative Consequences

1. **Complexity**: Multiple LUN detection requires monitoring and abort mechanisms
2. **User Experience**: Users must manually split LUNs before re-initiating migration
3. **Limitations**: Restricts fan-in scenarios that some customers may require
4. **Operational Overhead**: Requires allow-listing process for exceptions

### Technical Debt

1. **Multiple LUN Support**: Need to implement post-GA
2. **SVM Scaling**: Requires detailed planning for QOS and IP range management
3. **RBAC Improvements**: Dependent on ONTAP fixes for full-path RBAC
4. **Monitoring**: Need comprehensive monitoring for LUN detection during replication

## References

1. [ONTAPPM-109185 - Ability to restrict snapmirror from external peers on a per volume level](https://ontap.com/ONTAPPM-109185)