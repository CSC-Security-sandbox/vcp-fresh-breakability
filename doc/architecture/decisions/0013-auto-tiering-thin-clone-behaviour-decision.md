# ONTAP Auto Tiering Policy Behavior for Volume Clones

## Overview

- This document handles the design decision for clone creation of volumes with auto-tiering (AT) policy enabled parent volumes.
- The recommended approach is to create the clone first and then make a separate update call to set the desired AT policy for the clone.
- This change has been communicated and discussed with **Vignesh**.

## Approaches

### 1. Sync ONTAP status of AT policy in DB

**Pros:**
- We are not deviating from ONTAP default behaviour

**Cons:**
- Expensive
- Race condition - updating from ONTAP while customer triggered AT update
- Fails in pause
- Need to add extra checks in many flows

### 2. Ask CCFE to explicitly send clone policy either set by customer or default (parent's AT policy)

This won't be possible because ONTAP doesn't accept tiering policy for thin clones creation - will need to educate customers about this scenario that if they pass policy, request for thin clone will fail

### 3. Block AT policy acceptance during clone creation in all cases (parent with AT/without AT). Also clone AT policy can be updated later on

### 4. Accepting the AT policy for clone, by creating clone first and then making an update call for AT policy for the clone - **Recommended & Decided**

## Scenarios

### Scenario 1: Parent vol with AT set, clone vol w/o AT → ONTAP sets policy, VCP is not setting

```
vserver                         volume    tiering-policy 
--------------------------- --------- -------------- 
gcnv-a5de140756c8241-svm-01 parvolat1 auto
```

**DB:** `{"tiering_policy":"auto","retrieval_policy":"default","cooling_threshold_days":2,"hot_tier_bypass_mode_enabled":false}`

```
vserver                         volume         tiering-policy 
--------------------------- -------------- -------------- 
gcnv-a5de140756c8241-svm-01 parvolat1cnoat auto
```

**Result:** Clone - AT policy not set in DB

### Scenario 2: Parent vol with AT, clone vol with diff AT policy

Using same par vol as above, gave all policy for clone but ONTAP takes parent policy only

```
vserver                         volume           tiering-policy 
--------------------------- ---------------- -------------- 
gcnv-a5de140756c8241-svm-01 parvolat1cdiffat auto
```

**DB:** `{"tiering_policy":"all","retrieval_policy":"default","cooling_threshold_days":2,"cloud_write_mode_enabled":true,"hot_tier_bypass_mode_enabled":true}`

**Bug:** Policy is changing in DB but not possible in ONTAP

Code change to pass explicit diff policy with clone in ONTAP - fails to create clone - `"Specifying a value for \"tiering.policy\" is not valid for a volume FlexClone creation."` - ONTAP doesn't allow to change clone's AT policy

### Scenario 3: Clone vol with inherited AT policy from parent - Parvolat1cdiffat - updating AT policy

```
volume show -vserver gcnv-a5de140756c8241-svm-01 -volume parvolat1cdiffat -fields tiering-policy
vserver                         volume           tiering-policy 
--------------------------- ---------------- -------------- 
gcnv-a5de140756c8241-svm-01 parvolat1cdiffat all

volume show -vserver gcnv-a5de140756c8241-svm-01 -volume parvolat1 -fields tiering-policy
vserver                         volume    tiering-policy 
--------------------------- --------- -------------- 
gcnv-a5de140756c8241-svm-01 parvolat1 auto
```

**Result:** Update operation is allowed in ONTAP and gets reflected in DB as well

### Scenario 4: Parent vol with AT, clone vol with explicitly passing same AT policy as parent

**Error:** `"Specifying a value for \"tiering.policy\" is not valid for a volume FlexClone creation."`

### Scenario 5: Parent vol with AT, clone vol with AT, parent AT disabled, clone should not be affected

```
volume show -vserver gcnv-a5de140756c8241-svm-01 -volume parvolat1 -fields tiering-policy
vserver                         volume    tiering-policy 
--------------------------- --------- -------------- 
gcnv-a5de140756c8241-svm-01 parvolat1 none

volume show -vserver gcnv-a5de140756c8241-svm-01 -volume parvolat1cdiffat -fields tiering-policy
vserver                         volume           tiering-policy 
--------------------------- ---------------- -------------- 
gcnv-a5de140756c8241-svm-01 parvolat1cdiffat auto
```

**Result:** Clone not affected

### Scenario 6: Parent vol with AT, clone vol with AT, parent AT changed, clone should not be affected

```
volume show -vserver gcnv-a5de140756c8241-svm-01 -volume parvolat1 -fields tiering-policy
vserver                         volume    tiering-policy 
--------------------------- --------- -------------- 
gcnv-a5de140756c8241-svm-01 parvolat1 all

volume show -vserver gcnv-a5de140756c8241-svm-01 -volume parvolat1cdiffat -fields tiering-policy
vserver                         volume           tiering-policy 
--------------------------- ---------------- -------------- 
gcnv-a5de140756c8241-svm-01 parvolat1cdiffat auto
```

**Result:** Clone not affected

### Scenario 7: Parent vol w/o AT, clone with AT

**Error:** `"Specifying a value for \"tiering.policy\" is not valid for a volume FlexClone creation."`

### Scenario 8: Parent vol w/o AT, clone with AT, parent updated with AT

Not possible because of Scenario 7.

### Scenario 9: Parent with AT policy, clone w/o AT policy, update clone with a diff AT policy

Same as Scenario 2.

### Scenario 10: Parent vol AT changed, clone with no AT (changed AT/same AT)

```
volume show -vserver gcnv-a5de140756c8241-svm-01 -volume parvol5 -fields tiering-policy
vserver                         volume  tiering-policy 
--------------------------- ------- -------------- 
gcnv-a5de140756c8241-svm-01 parvol5 auto

volume show -vserver gcnv-a5de140756c8241-svm-01 -volume parvol5 -fields tiering-policy
vserver                         volume  tiering-policy 
--------------------------- ------- -------------- 
gcnv-a5de140756c8241-svm-01 parvol5 all

volume show -vserver gcnv-a5de140756c8241-svm-01 -volume parvol5c1 -fields tiering-policy
vserver                         volume    tiering-policy 
--------------------------- --------- -------------- 
gcnv-a5de140756c8241-svm-01 parvol5c1 all
```

**Result:** Clone inherits AT policy of parent vol

### Scenario 11: Parent vol AT changed, clone with no AT (changed AT/same AT), updated AT of clone

Allowed

### Scenario 12: Parent vol has some data in hot tier, with same shared data clone is created with all policy, check data should not be moved

Check behaviour on reading data

### Scenario 13: Parent with AT, made a clone, clone of clone

```
volume show -vserver gcnv-a5de140756c8241-svm-01 -volume parvolat4 -fields tiering-policy
vserver                         volume    tiering-policy 
--------------------------- --------- -------------- 
gcnv-a5de140756c8241-svm-01 parvolat4 auto

volume show -vserver gcnv-a5de140756c8241-svm-01 -volume parvolat4c3 -fields tiering-policy
vserver                         volume      tiering-policy 
--------------------------- ----------- -------------- 
gcnv-a5de140756c8241-svm-01 parvolat4c3 auto

volume show -vserver gcnv-a5de140756c8241-svm-01 -volume parvolat4c3c1 -fields tiering-policy
vserver                         volume        tiering-policy 
--------------------------- ------------- -------------- 
gcnv-a5de140756c8241-svm-01 parvolat4c3c1 auto
```

### Scenario 14: Parent with AT, made a clone, clone updated with diff AT, clone of clone

```
volume show -vserver gcnv-a5de140756c8241-svm-01 -volume parvolat4 -fields tiering-policy
vserver                         volume    tiering-policy 
--------------------------- --------- -------------- 
gcnv-a5de140756c8241-svm-01 parvolat4 auto

volume show -vserver gcnv-a5de140756c8241-svm-01 -volume parvolat4clone -fields tiering-policy
There are no entries matching your query.

volume show -vserver gcnv-a5de140756c8241-svm-01 -volume parvolat4clone1 -fields tiering-policy
vserver                         volume          tiering-policy 
--------------------------- --------------- -------------- 
gcnv-a5de140756c8241-svm-01 parvolat4clone1 auto

volume show -vserver gcnv-a5de140756c8241-svm-01 -volume parvolat4clone1 -fields tiering-policy
vserver                         volume          tiering-policy 
--------------------------- --------------- -------------- 
gcnv-a5de140756c8241-svm-01 parvolat4clone1 all

volume show -vserver gcnv-a5de140756c8241-svm-01 -volume parvolat4clone1c -fields tiering-policy
vserver                         volume            tiering-policy 
--------------------------- ----------------- -------------- 
gcnv-a5de140756c8241-svm-01 parvolat4clone1c1 all
```

## Decision

**Approach 4** - Accepting the AT policy for clone, by creating clone first and then making an update call for AT policy for the clone

## Validated Scenarios

### 1. Auto AT policy in file vol, no AT policy passed in clone volume

```
volume show -vserver gcnv-a5de140756c8241-svm-01 -volume autoparvol -fields tiering-policy
vserver                     volume     tiering-policy 
--------------------------- ---------- -------------- 
gcnv-a5de140756c8241-svm-01 autoparvol auto           
```

**DB:** `{"tiering_policy":"auto","retrieval_policy":"default","cooling_threshold_days":2,"hot_tier_bypass_mode_enabled":false}`

```
gcnv-a5de140756c8241::> volume show -vserver gcnv-a5de140756c8241-svm-01 -volume autoparvolclnoat -fields tiering-policy
vserver                     volume           tiering-policy 
--------------------------- ---------------- -------------- 
gcnv-a5de140756c8241-svm-01 autoparvolclnoat none
```

**Result:** `auto_tiering_policy -> NONE`

### 2. Auto AT policy in file vol, all AT policy in clone

Parent vol same as above

```
volume show -vserver gcnv-a5de140756c8241-svm-01 -volume autoparvolclallat -fields tiering-policy
vserver                     volume            tiering-policy 
--------------------------- ----------------- -------------- 
gcnv-a5de140756c8241-svm-01 autoparvolclallat all
```

**DB:** `{"tiering_policy":"all","retrieval_policy":"default","cooling_threshold_days":2,"cloud_write_mode_enabled":true,"hot_tier_bypass_mode_enabled":true}`

### 3. Auto AT policy in file vol, auto AT policy in clone

```
volume show -vserver gcnv-a5de140756c8241-svm-01 -volume autoparvolclautoat -fields tiering-policy
vserver                     volume             tiering-policy 
--------------------------- ------------------ -------------- 
gcnv-a5de140756c8241-svm-01 autoparvolclautoat auto
```

**DB:** `{"tiering_policy":"auto","retrieval_policy":"default","cooling_threshold_days":2,"hot_tier_bypass_mode_enabled":false}`

### 4. Auto AT policy in file vol, pause AT policy in clone

```
volume show -vserver gcnv-a5de140756c8241-svm-01 -volume autoparvolclpauseat -fields tiering-policy
vserver                     volume              tiering-policy 
--------------------------- ------------------- -------------- 
gcnv-a5de140756c8241-svm-01 autoparvolclpauseat none
```

**DB:** `{"tiering_policy":"none","retrieval_policy":"","cooling_threshold_days":2,"cloud_write_mode_enabled":false,"hot_tier_bypass_mode_enabled":false}`

### 5. All AT policy in file vol, no AT policy passed in clone

```
volume show -vserver gcnv-a5de140756c8241-svm-01 -volume allparvol -fields tiering-policy           
vserver                     volume    tiering-policy 
--------------------------- --------- -------------- 
gcnv-a5de140756c8241-svm-01 allparvol all
```

**DB:** `{"tiering_policy":"all","retrieval_policy":"default","cooling_threshold_days":2,"cloud_write_mode_enabled":true,"hot_tier_bypass_mode_enabled":true}`

```
volume show -vserver gcnv-a5de140756c8241-svm-01 -volume allparvolclnoat -fields tiering-policy  
vserver                     volume          tiering-policy 
--------------------------- --------------- -------------- 
gcnv-a5de140756c8241-svm-01 allparvolclnoat none
```

### 6. All AT policy in file vol, all AT policy in clone

```
volume show -vserver gcnv-a5de140756c8241-svm-01 -volume allparvolclallat -fields tiering-policy
vserver                     volume            tiering-policy 
--------------------------- ---------------- -------------- 
gcnv-a5de140756c8241-svm-01 allparvolclallat all
```

**DB:** `{"tiering_policy":"all","retrieval_policy":"default","cooling_threshold_days":2,"cloud_write_mode_enabled":true,"hot_tier_bypass_mode_enabled":true}`

### 7. All AT policy in file vol, auto AT policy in clone

**Error:** `"Only the \"all\" tiering policy is allowed on a volume with cloud write."` --> handling this scenario via validation in orchestration and throwing 400

**Note:** During volume create flow, when creating a clone with an auto autotiering (AT) policy from a parent volume that has an "all" AT policy, the clone creation fails. However, if we later update the clone volume of that same parent volume (with all AT policy) to use an auto AT policy via the update volume API, the operation succeeds.

### 8. All AT policy in file vol, pause AT policy in clone

```
volume show -vserver gcnv-a5de140756c8241-svm-01 -volume allparvolclpauseat -fields tiering-policy
vserver                     volume              tiering-policy 
--------------------------- ------------------- -------------- 
gcnv-a5de140756c8241-svm-01 allparvolclpauseat none
```

**DB:** `{"tiering_policy":"none","retrieval_policy":"","cooling_threshold_days":2,"cloud_write_mode_enabled":false,"hot_tier_bypass_mode_enabled":false}`

### 9. No AT policy in file vol, all AT policy in clone

```
volume show -vserver gcnv-a5de140756c8241-svm-01 -volume noparvol -fields tiering-policy           
vserver                     volume   tiering-policy 
--------------------------- -------- -------------- 
gcnv-a5de140756c8241-svm-01 noparvol none
```

```
volume show -vserver gcnv-a5de140756c8241-svm-01 -volume noparvolclallat -fields tiering-policy
vserver                     volume          tiering-policy 
--------------------------- --------------- -------------- 
gcnv-a5de140756c8241-svm-01 noparvolclallat all
```

**DB:** `{"tiering_policy":"all","retrieval_policy":"default","cooling_threshold_days":2,"cloud_write_mode_enabled":true,"hot_tier_bypass_mode_enabled":true}`

### 10. No AT policy in file vol, auto AT policy in clone

```
volume show -vserver gcnv-a5de140756c8241-svm-01 -volume noparvolclautoat -fields tiering-policy
vserver                     volume            tiering-policy 
--------------------------- ---------------- -------------- 
gcnv-a5de140756c8241-svm-01 noparvolclautoat auto
```

**DB:** `{"tiering_policy":"auto","retrieval_policy":"default","cooling_threshold_days":2,"hot_tier_bypass_mode_enabled":false}`

### 11. No AT policy in file vol, no AT policy passed in clone

```
volume show -vserver gcnv-a5de140756c8241-svm-01 -volume noparvolclnoat -fields tiering-policy  
vserver                     volume         tiering-policy 
--------------------------- -------------- -------------- 
gcnv-a5de140756c8241-svm-01 noparvolclnoat none
```

### 12. No AT policy in file vol, pause AT policy in clone

```
volume show -vserver gcnv-a5de140756c8241-svm-01 -volume noparvolclpauseat -fields tiering-policy
vserver                     volume             tiering-policy 
--------------------------- ------------------ -------------- 
gcnv-a5de140756c8241-svm-01 noparvolclpauseat none
```

**DB:** `{"tiering_policy":"none","retrieval_policy":"","cooling_threshold_days":2,"cloud_write_mode_enabled":false,"hot_tier_bypass_mode_enabled":false}`

### 13. Pause AT policy in file vol, all AT policy in clone

```
volume show -vserver gcnv-a5de140756c8241-svm-01 -volume pauseparvol -fields tiering-policy      
vserver                     volume      tiering-policy 
--------------------------- ----------- -------------- 
gcnv-a5de140756c8241-svm-01 pauseparvol none           
```

**DB:** `{"tiering_policy":"none","retrieval_policy":"","cooling_threshold_days":2,"cloud_write_mode_enabled":false,"hot_tier_bypass_mode_enabled":false}`

```
volume show -vserver gcnv-a5de140756c8241-svm-01 -volume pauseparvolclallat -fields tiering-policy
vserver                     volume              tiering-policy 
--------------------------- ------------------- -------------- 
gcnv-a5de140756c8241-svm-01 pauseparvolclallat all
```

**DB:** `{"tiering_policy":"all","retrieval_policy":"default","cooling_threshold_days":2,"cloud_write_mode_enabled":true,"hot_tier_bypass_mode_enabled":true}`

### 14. Pause AT policy in file vol, auto AT policy in clone

```
volume show -vserver gcnv-a5de140756c8241-svm-01 -volume pauseparvolclautoat -fields tiering-policy
vserver                     volume               tiering-policy 
--------------------------- -------------------- -------------- 
gcnv-a5de140756c8241-svm-01 pauseparvolclautoat auto
```

**DB:** `{"tiering_policy":"auto","retrieval_policy":"default","cooling_threshold_days":2,"hot_tier_bypass_mode_enabled":false}`

### 15. Pause AT policy in file vol, no AT policy passed in clone

```
volume show -vserver gcnv-a5de140756c8241-svm-01 -volume pauseparvolclnoat -fields tiering-policy  
vserver                     volume             tiering-policy 
--------------------------- ------------------ -------------- 
gcnv-a5de140756c8241-svm-01 pauseparvolclnoat none
```

### 16. Pause AT policy in file vol, pause AT policy in clone

**DB:** `{"tiering_policy":"none","retrieval_policy":"","cooling_threshold_days":2,"cloud_write_mode_enabled":false,"hot_tier_bypass_mode_enabled":false}`

```
volume show -vserver gcnv-a5de140756c8241-svm-01 -volume pauseparvolclpauseat -fields tiering-policy
vserver                     volume               tiering-policy 
--------------------------- -------------------- -------------- 
gcnv-a5de140756c8241-svm-01 pauseparvolclpauseat none
```

### 17. Auto AT policy in block vol, auto AT policy in clone

```
volume show -vserver gcnv-a5de140756c8241-svm-01 -volume autobvol -fields tiering-policy            
vserver                     volume   tiering-policy 
--------------------------- -------- -------------- 
gcnv-a5de140756c8241-svm-01 autobvol snapshot-only
```

**DB:** `{"tiering_policy":"snapshot_only","retrieval_policy":"default","cooling_threshold_days":2,"hot_tier_bypass_mode_enabled":false}`

```
volume show -vserver gcnv-a5de140756c8241-svm-01 -volume autobvolclauto -fields tiering-policy
vserver                     volume          tiering-policy 
--------------------------- -------------- -------------- 
gcnv-a5de140756c8241-svm-01 autobvolclauto snapshot-only
```

**DB:** `{"tiering_policy":"snapshot_only","retrieval_policy":"default","cooling_threshold_days":2,"hot_tier_bypass_mode_enabled":false}`

### 18. Auto AT policy in block vol, all AT policy in clone

All AT policy not allowed for block volume

### 19. Auto AT policy in block vol, no AT policy passed in clone

```
volume show -vserver gcnv-a5de140756c8241-svm-01 -volume autobvolclno -fields tiering-policy  
vserver                     volume        tiering-policy 
--------------------------- ------------- -------------- 
gcnv-a5de140756c8241-svm-01 autobvolclno none
```

### 20. Auto AT policy in block vol, pause AT policy in clone

```
volume show -vserver gcnv-a5de140756c8241-svm-01 -volume autobvolclpause -fields tiering-policy
vserver                     volume           tiering-policy 
--------------------------- --------------- -------------- 
gcnv-a5de140756c8241-svm-01 autobvolclpause none
```

**DB:** `{"tiering_policy":"none","retrieval_policy":"","cooling_threshold_days":2,"cloud_write_mode_enabled":false,"hot_tier_bypass_mode_enabled":false}`

### 21. No AT policy in block, all AT policy in clone

```
volume show -vserver gcnv-a5de140756c8241-svm-01 -volume noatbvol -fields tiering-policy       
vserver                     volume   tiering-policy 
--------------------------- -------- -------------- 
gcnv-a5de140756c8241-svm-01 noatbvol none
```

All AT blocked in blocks vol

### 22. No AT policy in block, auto AT policy in clone

```
volume show -vserver gcnv-a5de140756c8241-svm-01 -volume noatbvolclauto -fields tiering-policy
vserver                     volume          tiering-policy 
--------------------------- -------------- -------------- 
gcnv-a5de140756c8241-svm-01 noatbvolclauto snapshot-only
```

**DB:** `{"tiering_policy":"snapshot_only","retrieval_policy":"default","cooling_threshold_days":2,"hot_tier_bypass_mode_enabled":false}`

### 23. No AT policy in block, no AT policy passed in clone

```
volume show -vserver gcnv-a5de140756c8241-svm-01 -volume noatbvolclno -fields tiering-policy  
vserver                     volume        tiering-policy 
--------------------------- ------------- -------------- 
gcnv-a5de140756c8241-svm-01 noatbvolclno none
```

### 24. No AT policy in block, pause AT policy in clone

```
volume show -vserver gcnv-a5de140756c8241-svm-01 -volume noatbvolclpause -fields tiering-policy
vserver                     volume           tiering-policy 
--------------------------- --------------- -------------- 
gcnv-a5de140756c8241-svm-01 noatbvolclpause none
```

**DB:** `{"tiering_policy":"none","retrieval_policy":"","cooling_threshold_days":2,"cloud_write_mode_enabled":false,"hot_tier_bypass_mode_enabled":false}`

### 25. Pause AT policy in block, all AT policy in clone

```
volume show -vserver gcnv-a5de140756c8241-svm-01 -volume pausebvol -fields tiering-policy      
vserver                     volume    tiering-policy 
--------------------------- --------- -------------- 
gcnv-a5de140756c8241-svm-01 pausebvol none
```

**DB:** `{"tiering_policy":"none","retrieval_policy":"","cooling_threshold_days":2,"cloud_write_mode_enabled":false,"hot_tier_bypass_mode_enabled":false}`

All AT policy not allowed for blocks volume

### 26. Pause AT policy in block, auto AT policy in clone

```
volume show -vserver gcnv-a5de140756c8241-svm-01 -volume pausebvolclauto -fields tiering-policy 
vserver                     volume           tiering-policy 
--------------------------- --------------- -------------- 
gcnv-a5de140756c8241-svm-01 pausebvolclauto snapshot-only
```

**DB:** `{"tiering_policy":"snapshot_only","retrieval_policy":"default","cooling_threshold_days":2,"hot_tier_bypass_mode_enabled":false}`

### 27. Pause AT policy in block, no AT policy passed in clone

```
volume show -vserver gcnv-a5de140756c8241-svm-01 -volume pausebvolclno -fields tiering-policy  
vserver                     volume         tiering-policy 
--------------------------- -------------- -------------- 
gcnv-a5de140756c8241-svm-01 pausebvolclno none
```

### 28. Pause AT policy in block, pause AT policy in clone

```
volume show -vserver gcnv-a5de140756c8241-svm-01 -volume pausebvolclpause1 -fields tiering-policy
vserver                     volume             tiering-policy 
--------------------------- ------------------ -------------- 
gcnv-a5de140756c8241-svm-01 pausebvolclpause1 none
```

**DB:** `{"tiering_policy":"none","retrieval_policy":"","cooling_threshold_days":2,"cloud_write_mode_enabled":false,"hot_tier_bypass_mode_enabled":false}`

