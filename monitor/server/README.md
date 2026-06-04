# Building a new image for con-mon web server

Once a podman image is built for the con-mon web server -- aka the CE
(Consistency Enforcement) monitoring web server, and pushed into the
CERN registry, the local script test-mac.sh can be used to
conveniently launch the podman image on a MacBook (laptop), for
testing purposes.  Here is the cheat-sheet:

```console
#.. on your fermicloud node:
cd <path>/cms_consistency
cd monitor/server

#.. podman setup
podman login registry.cern.ch
  <username + pwd>

#.. if test data is needed prepare it before building the image - see later
<prepare test data>

#.. then update image tags on build.sh, start.sh as needed
source ./build.sh
```

# Testing the con-mon image locally

In order to test a new con-mon image, it is recommented to launch the
new image locally an check the web pages directly on a local browser.
The new image can be even run on a MacBook, but in this case a *Podman
Machine* is required to be running (more about this podman machine
later below).  Here is the verified cheat-sheet:

```
#.. on your mac laptop
cd <path>/cms_consistency/monitor/server

#.. update image tag on mac-test.sh, then launch it -- rinse and repeat :-)
podman machine start <vm_name>
source ./mac-test.sh

#.. then on a browser, navigate to http://localhost:8080/ce/index
open http://localhost:8080/ce/index

#.. once you're done with the local testing stop the podman machine
podman machine stop <vm_name>
```

# Prepare test data for local testing of the con-mon web server image

This test data is required for showing something on the local testing.
Here is a sequence of commands to execute on the consistency pod
to prepare a tarball, and the sequence of transfers to get it on the
right place to get it into the web server image:

On the consistency pod:

```console
    cd /var/cache/
    tar czf /var/cache/test/somedata.tgz \
      consistency-dump/T2_BR_UERJ* \
      consistency-dump/T2_TW_NCHC* \
      consistency-dump/T3_US_Colorado*

    scp /var/cache/test/somedata.tgz <cuser>@lxplus.cern.ch:Downloads/
```

Then on the laptop:
```console
    scp <cuser>@lxplus.cern.ch:x/somedata.tgz <fuser>@fnalu.fnal.gov:.
```

Then from fermicloud:
```console
    cd <path>/cms_consistency/monitor/server
    scp <fuser>@fnalu.fnal.gov:somedata.tgz .

    cd reports

    tar xzf ../somedata.tgz
    mv consistency-dump/* .
    mv consistency-dump unmerged  #.. empty
```

At this point, the test data is ready to be built into the con-mon
image, once the right lines are uncommented in the Dockerfile.  Follow
instructions on comments in that file.  Of course, don't forget to
clean up before a new image is prepared for release.


# Pushing images into (pre-)production

Once images are available from the CERN registry, they are also
available for release into the (pre-)production servers, which then
become visible at:

Pre-production: https://cmsweb-testbed.cern.ch/rucioconmon/ce/index

Production: https://cmsweb.cern.ch/rucioconmon/ce/index

## Requesting the release of a new image

For pre-production, visit:

https://github.com/dmwm/CMSKubernetes/edit/master/helm/rucio-con-mon/values-preprod.yaml

Modify the tag string as desired, then commit the changes (appropriately
documenting the changes in the tagged image), then create a pull request.

The same procedure applies for a release into the production server, but the
file `values-prod.yaml` should be used instead:

https://github.com/dmwm/CMSKubernetes/edit/master/helm/rucio-con-mon/values-prod.yaml

# What is a Podman Machine?

A Podman Machine is a lightweight, headless Linux virtual machine (VM)
managed automatically by Podman.

Because containers require a Linux kernel to run (utilizing Linux
features like namespaces and cgroups), Podman on macOS acts as a
client that talks to a Podman daemon running inside this Linux
VM. When you type podman run in your Mac terminal, the command is
securely tubed into the VM, where the actual container is launched.

[ Your Mac Terminal ]
   ---> [ Podman Client ]
      ---> [ Podman Machine (Linux VM) ]
          ---> [ Linux Container ]

By default, Podman uses Fedora CoreOS as the operating system for this
VM because it is lightweight, secure, and optimized for running
containers.

## Key Concepts & Architecture

### VM Providers (Hypervisors)

  Podman needs a hypervisor to run the VM on macOS. It supports two
  primary backends:

  Apple Hypervisor (vfkit): The default and highly recommended
  provider for modern Apple Silicon (M1/M2/M3/M4) Macs. It uses native
  macOS virtualization APIs for great performance.

  QEMU: The traditional backend, used automatically on older Intel-based Macs or as a fallback.

### Rootless by Default

  Sticking to Podman’s core philosophy, the Podman machine runs in
  rootless mode by default. This means the VM runs under your standard
  macOS user account without needing sudo privileges, drastically
  improving security compared to Docker Desktop's traditional setup.

### File Sharing & Volumes

  When you want to mount a folder from your Mac into a container
  (e.g., -v /Users/me/project:/app), Podman automatically handles the
  bridging. On macOS, it typically uses virtiofs (or sshfs on older
  setups) to mount your /Users directory into the Linux VM so the
  container can see your Mac's files.


## Essential Commands to Manage the Machine

You cannot run a Podman image until your Podman machine is created and
running. Here are the core lifecycle commands:

### Create the Machine

```Bash
podman machine init <vm_name>
```

This downloads the Fedora CoreOS image and configures the VM. You only need to do this once.

### Start the Machine

```Bash
podman machine start <vm_name>
```

You must run this after every Mac reboot before you can use Podman.

### Check the Status

```Bash
podman machine list
```

Shows you if the machine is running, how many resources it has, and if it is the default machine.

### Stop the Machine

```Bash
podman machine stop <vm_name>
```

Saves your Mac's battery and CPU when you aren't developing.


## Important Tips for Mac Users

### Resource Allocation

By default, Podman allocates a modest amount of CPU, memory, and disk
space to the VM. If you are running heavy images (like databases or
local AI models), you can customize this during initialization:

```Bash
podman machine init --cpus 4 --memory 4096 --disk-size 50
```

### Architecture Matching (Apple Silicon)

If you are on an M1/M2/M3/M4 Mac, Podman will pull **arm64** Linux
images by default. If you absolutely must run an Intel-based image
(**amd64**), Podman utilizes Apple's Rosetta 2 inside the VM to
emulate it, though you may need to explicitly pass the `--platform`
linux/amd64 flag in your run command.

### Port Forwarding

When a container exposes a port (e.g., -p 8080:80), Podman
automatically forwards that port from the Linux VM to your Mac's
localhost. You can open https://localhost:8080 in Safari or Chrome
just like normal.
