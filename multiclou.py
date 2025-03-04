import os
import requests
import base64
import subprocess
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Configuration
WORKSPACE_CONFIG = {
    "AWS": {
        "url": os.getenv("AWS_WORKSPACE_URL"),
        "token": os.getenv("AWS_ACCESS_TOKEN"),
    },
    "AZURE": {
        "url": os.getenv("AZURE_WORKSPACE_URL"),
        "token": os.getenv("AZURE_ACCESS_TOKEN"),
    },
    "GCP": {
        "url": os.getenv("GCP_WORKSPACE_URL"),
        "token": os.getenv("GCP_ACCESS_TOKEN"),
    },
}

NOTEBOOK_DIR = os.getenv("NOTEBOOK_DIR")
EMAIL_TO_GRANT = os.getenv("EMAIL_TO_GRANT")
USER_TO_SYNC = os.getenv("USER_TO_SYNC")


def get_headers(token):
    return {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    }


def get_workspace_config(cloud_provider):
    return WORKSPACE_CONFIG.get(cloud_provider.upper(), None)


def pull_notebooks_from_git(git_url, target_dir):
    try:
        subprocess.run(["git", "clone", git_url, target_dir], check=True)
        print(f"Notebooks pulled from Git repository: {git_url}")
    except subprocess.CalledProcessError as e:
        print(f"Error pulling notebooks from Git: {e}")
        print("Please ensure you have the correct access rights and the repository exists.")
        print("If using SSH, ensure your SSH key is added to the SSH agent.")
        print("If using HTTPS, ensure you have the correct credentials.")
        return False
    return True


def list_notebooks(workspace_url, access_token, path):
    api_endpoint = f"{workspace_url}/api/2.0/workspace/list"
    try:
        response = requests.get(api_endpoint, headers=get_headers(access_token), params={"path": path})
        response.raise_for_status()
        return response.json().get("objects", [])
    except requests.exceptions.RequestException as e:
        print(f"Error listing notebooks in {workspace_url}: {e}")
        return []


def export_notebook(workspace_url, access_token, path):
    api_endpoint = f"{workspace_url}/api/2.0/workspace/export"
    try:
        response = requests.get(api_endpoint, headers=get_headers(access_token), params={"path": path, "format": "SOURCE"})
        response.raise_for_status()
        return base64.b64decode(response.json()["content"]).decode("utf-8")
    except requests.exceptions.RequestException as e:
        print(f"Error exporting notebook {path} from {workspace_url}: {e}")
        return None


def import_notebook(workspace_url, access_token, content, path, language='PYTHON'):
    api_endpoint = f"{workspace_url}/api/2.0/workspace/import"
    data = {
        "path": path,
        "format": "SOURCE",
        "language": language,
        "content": base64.b64encode(content.encode("utf-8")).decode("utf-8"),
        "overwrite": True
    }
    try:
        response = requests.post(api_endpoint, headers=get_headers(access_token), json=data)
        response.raise_for_status()
        print(f"Notebook imported successfully to {path} in {workspace_url}")
    except requests.exceptions.RequestException as e:
        print(f"Error importing notebook to {path} in {workspace_url}: {e}")


def get_object_status(workspace_url, access_token, path):
    api_endpoint = f"{workspace_url}/api/2.0/workspace/get-status"
    try:
        response = requests.get(api_endpoint, headers=get_headers(access_token), params={"path": path})
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        print(f"Error getting object status for {path} in {workspace_url}: {e}")
        return None


def get_permissions(workspace_url, access_token, path):
    object_status = get_object_status(workspace_url, access_token, path)
    if not object_status:
        return None

    object_id = object_status.get("object_id")
    object_type = object_status.get("object_type")

    if object_type == "NOTEBOOK":
        api_permissions = f"{workspace_url}/api/2.0/permissions/notebooks/{object_id}"
    elif object_type == "DIRECTORY":
        api_permissions = f"{workspace_url}/api/2.0/permissions/directories/{object_id}"
    else:
        print(f"Unsupported object type: {object_type}")
        return None

    try:
        response = requests.get(api_permissions, headers=get_headers(access_token))
        response.raise_for_status()
        permissions = response.json()

        # Extract and map permissions correctly
        acl = permissions.get("access_control_list", [])
        user_permissions = {}
        for item in acl:
            user_name = item.get("user_name")
            # Check for nested permission levels
            all_permissions = item.get("all_permissions", [])
            if all_permissions:
                permission_level = all_permissions[0].get("permission_level", "UNKNOWN")
            else:
                permission_level = item.get("permission_level", "UNKNOWN")
            user_permissions[user_name] = permission_level

        return user_permissions
    except requests.exceptions.RequestException as e:
        print(f"Error getting permissions for {path}: {e}")
        return None


def grant_permissions(workspace_url, access_token, path, email, permission_level, cluster_id=None):
    object_status = get_object_status(workspace_url, access_token, path)
    if not object_status:
        print(f"Skipping permissions for {path} - object status not found.")
        return False

    object_id = object_status.get("object_id")
    object_type = object_status.get("object_type")

    if object_type == "NOTEBOOK":
        api_permissions = f"{workspace_url}/api/2.0/permissions/notebooks/{object_id}"
    elif object_type == "DIRECTORY":
        api_permissions = f"{workspace_url}/api/2.0/permissions/directories/{object_id}"
    else:
        print(f"Skipping permissions for {path} - unsupported object type: {object_type}")
        return False

    data = {
        "access_control_list": [
            {
                "user_name": email,
                "permission_level": permission_level
            }
        ]
    }

    if cluster_id:
        data["access_control_list"].append({
            "user_name": email,
            "permission_level": "CAN_ATTACH_TO",
            "cluster_id": cluster_id
        })

    method = "PATCH" if "databricks.com" in workspace_url else "PUT"

    try:
        if method == "PATCH":
            response = requests.patch(api_permissions, headers=get_headers(access_token), json=data)
        else:
            response = requests.put(api_permissions, headers=get_headers(access_token), json=data)

        response.raise_for_status()
        print(f"Granted {permission_level} permissions to {email} for {path}")
        if cluster_id:
            print(f"Granted CAN_ATTACH_TO permissions to {email} for cluster {cluster_id}")
        return True
    except requests.exceptions.RequestException as e:
        print(f"Error granting permissions for {path}: {e}")
        return False


def sync_notebooks_and_permissions(source_cloud, target_cloud, git_url=None, cluster_id=None):
    print("Starting notebook synchronization and permission sync...")

    # Get workspace configurations
    source_config = get_workspace_config(source_cloud)
    target_config = get_workspace_config(target_cloud)

    if not source_config or not target_config:
        print("Invalid source or target cloud provider.")
        return

    # Pull notebooks from Git if URL is provided
    if git_url:
        if not pull_notebooks_from_git(git_url, NOTEBOOK_DIR):
            return  # Exit if Git pull fails

    # List permissions in the source workspace (GCP)
    print(f"Listing permissions in {source_cloud} workspace for directory: {NOTEBOOK_DIR}")
    source_permissions = get_permissions(source_config["url"], source_config["token"], NOTEBOOK_DIR)
    if source_permissions:
        print(f"Permissions in {source_cloud} workspace:")
        for user, permission in source_permissions.items():
            print(f"- {user}: {permission}")
    else:
        print(f"No permissions found in {source_cloud} workspace for directory: {NOTEBOOK_DIR}")

    # Sync notebooks and permissions
    source_notebooks = list_notebooks(source_config["url"], source_config["token"], NOTEBOOK_DIR)
    for nb in source_notebooks:
        if nb["object_type"] == "NOTEBOOK":
            content = export_notebook(source_config["url"], source_config["token"], nb["path"])
            if content is not None:
                import_notebook(target_config["url"], target_config["token"], content, nb["path"])

                # Sync permissions from GCP to Azure
                if source_permissions:
                    for user, permission_level in source_permissions.items():
                        grant_permissions(target_config["url"], target_config["token"], nb["path"], user, permission_level, cluster_id)

    # List permissions in the target workspace (Azure) after syncing
    print(f"Listing permissions in {target_cloud} workspace for directory: {NOTEBOOK_DIR}")
    target_permissions = get_permissions(target_config["url"], target_config["token"], NOTEBOOK_DIR)
    if target_permissions:
        print(f"Permissions in {target_cloud} workspace:")
        for user, permission in target_permissions.items():
            print(f"- {user}: {permission}")
    else:
        print(f"No permissions found in {target_cloud} workspace for directory: {NOTEBOOK_DIR}")

    print("Notebook synchronization and permission sync completed.")


if __name__ == "__main__":
    source_cloud = input("Enter source cloud provider (AWS/AZURE/GCP): ")
    target_cloud = input("Enter target cloud provider (AWS/AZURE/GCP): ")
    git_url = input("Enter Git repository URL (optional): ")
    cluster_id = input("Enter cluster ID for attach permissions (optional): ")

    sync_notebooks_and_permissions(source_cloud, target_cloud, git_url, cluster_id)