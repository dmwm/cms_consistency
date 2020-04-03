from rucio.client.rseclient import RSEClient
import uuid

c = RSEClient(account="ivm")
for rse in c.list_rses():
	u = uuid.UUID(rse["id"])
	print rse["rse"], u.hex
