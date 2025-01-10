## 1) install the databricks CLI:
```
curl -fsSL https://raw.githubusercontent.com/databricks/setup-cli/main/install.sh | sudo  sh
```

## 2) Create a personal token
To create a personal access token for the CLI, you can follow these steps:

Click on your user profile icon in the top bar of the Databricks workspace.
Select User Settings from the drop-down menu.
Click on Developer.
Next to Access tokens, click on Manage.
Click on Generate new token.
Optionally, you can enter a comment to help identify the token in the future and adjust the token's lifetime.
Click on Generate.
Copy the displayed token to a secure location.
Click on Done.
Remember to save the copied token in a secure location and not share it with others. If you lose the token, you will need to repeat the procedure to create a new one. If you believe the token has been compromised, it is recommended to delete it immediately by clicking the trash can (Revoke) icon next to the token on the Access tokens page.

Please note that if you are unable to create or use tokens in your workspace, it may be because your workspace administrator has disabled tokens or has not given you permission to create or use them. You can consult your workspace administrator or refer to the documentation on Enable or disable personal access token authentication for the workspace and Personal access token permissions for more information.


## 3) set up the databricks CLI
```
$ databricks configure
✔ Databricks host: https://adb-xxxxxxxxxxxxxxxxxx.14.azuredatabricks.net/█
Personal access token: ************************************
```    

## 4) create a secret scope
Because we don't have the premium plan, we need to set the initial-manage-principal to users:

```
databricks secrets create-scope pv --initial-manage-principal users
```

## 5) create the secret (e.g. add a SSAS token):
```
databricks secrets put-secret pv raw-sas-token --string-value "<sas_token>"
databricks secrets put-secret pv medallion-sas-token --string-value "<sas_token>"
```

