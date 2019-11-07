---
title: Decommission Bookies
---

In case the user wants to decommission a bookie, the following process is useful to follow in order to verify if the
decommissioning was safely done.

### Before we decommission
1. Ensure state of your cluster can support the decommissioning of the target bookie.<br>
Check if `EnsembleSize >= Write Quorum >= Ack Quorum` stays true with one less bookie
2. Ensure target bookie shows up in the listbookies command.
3. Ensure that there is no other process ongoing (upgrade etc).

### Process of Decommissioning
1. Log on to the bookie node, check if there are underreplicated ledgers.<br>
If there are, the decommission command will force them to be replicated.
<br>`$ bin/bookkeeper shell listunderreplicated`
2. Stop the bookie
<br>`$ bin/bookkeeper-daemon.sh stop bookie`

3. Run the decommission command.<br>
If you have logged onto the node you wish to decommission, you don't need to provide `-bookieid`<br>
If you are running the decommission command for target bookie node from another bookie node you should mention 
the target bookie id in the arguments for `-bookieid`
<br>`$ bin/bookkeeper shell decommissionbookie`
<br>or
<br>`$ bin/bookkeeper shell decommissionbookie -bookieid {target bookieid}`

4. Validate that there are no ledgers on decommissioned bookie
<br>`$ bin/bookkeeper shell listledgers -bookieid {target bookieid}`

Last step to verify is you could run this command to check if the bookie you decommissioned doesn’t show up in list bookies:

```bash
./bookkeeper shell listbookies -rw -h
./bookkeeper shell listbookies -ro -h
```

