The images are automatically built by the github action .github/workflow/release-images.yml
on creating a tag of the form release-<major_version>.<minor_version>.<patch_version> (e.g. release-1.0.0)

This will create and upload the image with name `rucio-consistency:<major_version>.<minor_version>.<patch_version>` to the container repository.


To manually build the images, you can run the following commands:
1. Modify the `CONSISTENCY_VERSION` in the build_image.sh script to the desired version
2. Set the container repository url ( we use a harbour registry, `HARBOR`)   
3. Login to registry
    ```
    docker login <registry_url>
    ```
    or similarly for podman
    ```
    podman login <registry_url>
    ```
4. Run the `build_image.sh` script from the docker directory
    ```
    ./build_image.sh
    ```
