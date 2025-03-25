
# VSA Cluster Deployment on GCP

This guide provides instructions for configuring and deploying a VSA cluster on Google Cloud Platform.


---

## **Configuration**

### **1. Update Project ID**
1. Open the `resource_deployer.go` file.
2. Locate the `projectId` variable and update it with your GCP project ID:
   ```go
   projectId := "your-gcp-project-id"
   ```
   Replace `"your-gcp-project-id"` with the actual ID of your GCP project where the VSA cluster will be deployed.

---

### **2. Setup VPC**

1. Create required VPC and subnet configurations in your tenant project.
      chmod +x setup-vpc.sh

      ./setup-vpc.sh
---

### **3. Copy Machine Image to Tenant Project**
1. Copy the required machine image to your GCP project (`projectId`).
2. Use the `gcloud` command to copy the image:
   ```bash
   gcloud compute images create <new-image-name> --source-image=<source-image-url> --project=<your-gcp-project-id>
   ```
   Replace:
    - `<new-image-name>`: The name of the new image in your project.
    - `<source-image-url>`: The URL of the source image provided by the VSA team.
    - `<your-gcp-project-id>`: Your GCP project ID.
- Note : This can be done using the GCP console as well.


---

### **4. Update Image Details**
2. Update the `sample_yaml.yaml` file with the new image details:
   ```yaml
   mediatorImage: "<mediator-image-name>"
   sourceImage: "<source-image-name>"
   ```

---

## **Usage**

### **Create a Deployment**
To create a deployment, use the following command:
```bash
go run resource_deployer.go <deployment-name>
```

#### Example:
```bash
go run resource_deployer.go test-deployment
```
