ssh -i ~/.ssh/gcp sevin@<external ip>

or

edit config

nano config

HostName <external ip>
  User sevin
  IdentityFile ~/.ssh/gcp

  and then 

  ssh taxiproject