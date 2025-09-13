# Launching Integration Tests

Before running integration tests, you need to set up and license Confluence instances, since there is no free
installation option available.

---

## 1. Obtain a Confluence license

- Get a **Confluence Data Center trial license (30 days)** from  
  [https://www.atlassian.com/purchase/my/licenses/](https://www.atlassian.com/purchase/my/licenses/).
- A `ServerID` is normally available only after starting a Confluence instance.  
  However, you can provide it in advance via an environment variable.
  In `docker-compose.yaml`, use the example `ATL_PRODUCT_SERVER_ID` value from any `confluence-*` service.

---

## 2. Configure environment variables

- Copy `.env.confluence.example` to `.env.confluence`.
- Add the license key to the corresponding variable inside the file.

---

## 3. Start Confluence instances

Run:

```bash
docker-compose up -d
```

Then open:

- http://localhost:8090
- http://localhost:9090

Follow the setup wizard for each instance:

- Select `non-clustered` deployment.
- Choose `Empty Site`.
- Select `Manage users` and create a user account. This account will be used to run the `confluence-sync` tests.
- Create a new space with any name.

---

## 4. Install the Draw.io plugin

Some tests require the `draw.io` plugin, so it must be installed in both Confluence instances.

- Go to `Confluence Administration` → `Manage apps` → `Find new apps`. Search for `draw.io` and click `Free trial`.
- After installation, you will be prompted to obtain a trial license. Follow the instructions.
- Once you have the license, go back to the `Manage apps` page, expand the `draw.io` app settings, and activate the
  plugin.
  _Note: The same trial license key can be reused for both instances._

---

## 5. Run the integration tests

- Copy `.env.test.example` to `.env.test`.
- Fill in the required variables with the correct values.
- Load the environment variables and run the tests:

    ```bash
    set -a; source .env.test; set +a
    pytest integration
    ```
