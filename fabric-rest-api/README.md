# Using Microsoft Fabric Rest API to automate Fabric items creation.

The goal of this guide is to provide sample code for automating the creation of Microsoft Fabric items. This can be useful in various scenarios, such as CI/CD pipelines, automated environment setup for development, testing, and production, or establishing a baseline setup for organizational business units like marketing, sales, IT, and more.

**Resources**
- [Microsoft Fabric REST API documentation](https://learn.microsoft.com/en-us/rest/api/fabric/articles/)
- [Fabric API quickstart](https://learn.microsoft.com/en-us/rest/api/fabric/articles/get-started/fabric-api-quickstart)

### Prerequisites

**Permissions setup**

- You need to have Fabric capacity in order to obtain `capacity_id`. Follow this link to buy Fabric [capacity](https://learn.microsoft.com/en-us/fabric/enterprise/buy-subscription) or this to [enable Fabric for your Azure tenant](https://learn.microsoft.com/en-us/fabric/admin/fabric-switch).
- Follow [Fabric API quickstart](https://learn.microsoft.com/en-us/rest/api/fabric/articles/get-started/fabric-api-quickstart) to create app registration in Azure in order to obtain client_id, tenant_id, client_secret
- Enable **Allow service principals to use Fabric APIs** either for the entire organization or for the specific security group. Read more [here](https://learn.microsoft.com/en-us/power-bi/developer/embedded/embed-service-principal?tabs=azure-portal#step-3---enable-the-power-bi-service-admin-settings).
- In Fabric go to Settings > Admin portal > capacity settings > Fabric capacity > {click on your capacity} > contributor permissions.
  - Choose either the entire organization or specific user or groups. This to allow you to assign newly created workspaces to capacity.

**Code**
- Make sure to install dependencies > `pip install -r requirements.txt`
- We use [load_dotenv](https://pypi.org/project/python-dotenv/) to set environment variables with secrets.
- Add your values to `.env` file:

  ```
  CLIENT_ID=<replace with your client id>
  TENANT_ID=<replace with your tenant id>
  FABRIC_CLIENT_SECRET_VALUE=<replace with your secret value>
  AZURE_CAPACITY_ID=<replace with fabric capacity id>
  PRINCIPAL_ID_TO_ASSIGN=<replace with ID of your user>
  ```

## Getting started

Open notebook.ipynb and follow the steps.