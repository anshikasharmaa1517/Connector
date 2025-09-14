let currentPage = 1;
const formData = {};
let currentAuthType = "basic";

function updateFormData() {
  const inputs = document.querySelectorAll(
    'input[type="text"], input[type="password"], input[type="number"], select, textarea'
  );
  inputs.forEach((input) => {
    if (input.value) {
      formData[input.id] = input.value;
    }
  });

  // Capture checkbox states
  const checkboxes = document.querySelectorAll('input[type="checkbox"]');
  checkboxes.forEach((checkbox) => {
    formData[checkbox.id] = checkbox.checked;
  });
}

function updateSteps() {
  const steps = document.querySelectorAll(".step");
  steps.forEach((step, index) => {
    const stepNumber = index + 1;
    step.classList.remove("active", "completed");
    if (stepNumber === currentPage) {
      step.classList.add("active");
    } else if (stepNumber < currentPage) {
      step.classList.add("completed");
    }
  });
}

function goToPage(pageNumber) {
  // Validate page number
  if (pageNumber < 1 || pageNumber > 3) return;

  // Prevent unauthorized navigation
  if (pageNumber > currentPage) {
    // Check if trying to go past page 1 without authentication
    if (pageNumber > 1 && !sessionStorage.getItem("authenticationComplete")) {
      return;
    }

    // Check if trying to go past page 2
    if (pageNumber > 2) {
      const connectionName = document
        .getElementById("connectionName")
        .value.trim();
      if (!connectionName) {
        return;
      }
    }
  }

  // Hide all pages
  document.querySelectorAll(".page").forEach((page) => {
    page.classList.remove("active");
  });

  // Hide all navigation buttons
  document.querySelectorAll(".nav-buttons").forEach((nav) => {
    nav.style.display = "none";
  });

  // Show selected page
  document.getElementById(`page${pageNumber}`).classList.add("active");

  // Show corresponding navigation buttons
  document.getElementById(`page${pageNumber}-nav`).style.display = "flex";

  // Update current page and steps
  currentPage = pageNumber;
  updateSteps();
}

async function nextPage() {
  // If on page 1, ensure authentication is complete
  if (currentPage === 1) {
    if (!sessionStorage.getItem("authenticationComplete")) {
      const success = await testConnection();
      if (!success) {
        return;
      }
    }
    goToPage(2);
    return;
  }

  // For page 2
  if (currentPage === 2) {
    const connectionNameInput = document.getElementById("connectionName");
    const connectionName = connectionNameInput.value.trim();

    if (!connectionName) {
      connectionNameInput.style.border = "2px solid #DC2626";
      connectionNameInput.style.animation = "shake 0.5s";
      connectionNameInput.addEventListener("animationend", () => {
        connectionNameInput.style.animation = "";
      });
      connectionNameInput.addEventListener(
        "input",
        () => {
          connectionNameInput.style.border = "1px solid var(--border-color)";
        },
        { once: true }
      );
      return;
    }

    updateFormData();
    goToPage(3);
    return;
  }
}

function prevPage() {
  if (currentPage > 1) {
    goToPage(currentPage - 1);
  }
}

// Authentication type switching
function setupAuthenticationTypeSwitch() {
  const authButtons = document.querySelectorAll("#authTypeGroup .btn");
  const authSections = document.querySelectorAll(".auth-section");

  authButtons.forEach((button) => {
    button.addEventListener("click", () => {
      const authType = button.dataset.auth;

      // Update button states
      authButtons.forEach((btn) => {
        btn.classList.remove("btn-primary");
        btn.classList.add("btn-secondary");
      });
      button.classList.remove("btn-secondary");
      button.classList.add("btn-primary");

      // Update auth sections
      authSections.forEach((section) => {
        section.classList.remove("active");
      });
      document.getElementById(`${authType}-auth`).classList.add("active");

      currentAuthType = authType;
    });
  });
}

async function testConnection() {
  const testButton = document.getElementById("testConnectionBtn");
  const errorElement = document.getElementById("connectionResult");

  // Reset error state
  errorElement.classList.remove("visible");
  errorElement.textContent = "";

  // Get form values
  const host = document.getElementById("host").value.trim();
  const port = document.getElementById("port").value.trim();
  const protocol = document.getElementById("protocol").value;
  const ssl_verify = document.getElementById("ssl_verify").value === "true";
  const timeout = parseInt(document.getElementById("timeout").value) || 30;

  // Basic validation
  if (!host) {
    errorElement.textContent = "Host is required";
    errorElement.classList.add("visible");
    return false;
  }

  if (!port) {
    errorElement.textContent = "Port is required";
    errorElement.classList.add("visible");
    return false;
  }

  // Prepare credentials based on auth type
  let credentials = {
    host,
    port: parseInt(port),
    protocol,
    ssl_verify,
    timeout,
    auth_type: currentAuthType,
  };

  switch (currentAuthType) {
    case "basic":
      credentials.username = document.getElementById("username").value;
      credentials.password = document.getElementById("password").value;
      if (!credentials.username || !credentials.password) {
        errorElement.textContent =
          "Username and password are required for basic authentication";
        errorElement.classList.add("visible");
        return false;
      }
      break;

    case "api_key":
      credentials.api_key_id = document.getElementById("api_key_id").value;
      credentials.api_key = document.getElementById("api_key").value;
      if (!credentials.api_key_id || !credentials.api_key) {
        errorElement.textContent = "API Key ID and API Key are required";
        errorElement.classList.add("visible");
        return false;
      }
      break;

    case "bearer":
      credentials.token = document.getElementById("token").value;
      if (!credentials.token) {
        errorElement.textContent = "Bearer token is required";
        errorElement.classList.add("visible");
        return false;
      }
      break;
  }

  // Show loading state
  testButton.classList.add("loading");
  testButton.disabled = true;
  testButton.textContent = "Testing...";

  try {
    // Call test connection endpoint
    const response = await fetch("/workflows/v1/auth", {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
      },
      body: JSON.stringify({ credentials }),
    });

    const result = await response.json();

    if (result.success) {
      // Success - allow proceeding to next page
      sessionStorage.setItem("authenticationComplete", "true");
      sessionStorage.setItem("connectionData", JSON.stringify(credentials));

      errorElement.textContent = `✓ ${
        result.message || "Connected to Elasticsearch"
      }`;
      errorElement.className = "success-message visible";

      testButton.style.backgroundColor = "var(--success-color)";
      testButton.textContent = "Connection Successful";

      return true;
    } else {
      // Show error message
      errorElement.textContent =
        result.message ||
        "Failed to connect. Please check your credentials and try again.";
      errorElement.classList.add("visible");
      testButton.style.backgroundColor = "";
      return false;
    }
  } catch (error) {
    console.error("Connection test failed:", error);
    errorElement.textContent =
      "Connection test failed. Please check your settings and try again.";
    errorElement.classList.add("visible");
    testButton.style.backgroundColor = "";
    return false;
  } finally {
    // Reset button state
    testButton.classList.remove("loading");
    testButton.disabled = false;
    testButton.textContent = "Test Connection";
  }
}

async function startMetadataExtraction() {
  const startButton = document.getElementById("startExtractionBtn");
  const resultElement = document.getElementById("extractionResult");
  const workflowStatus = document.getElementById("workflowStatus");

  // Reset state
  resultElement.classList.remove("visible");
  resultElement.textContent = "";

  // Get all form data
  updateFormData();

  // Get connection data from previous steps
  const connectionData = JSON.parse(
    sessionStorage.getItem("connectionData") || "{}"
  );
  const connectionName = document.getElementById("connectionName").value.trim();

  if (!connectionName) {
    resultElement.textContent = "Connection name is required";
    resultElement.className = "status-message error visible";
    return;
  }

  // Prepare extraction configuration
  const extractionConfig = {
    credentials: connectionData,
    connection: {
      name: connectionName,
      description: document.getElementById("connectionDescription").value,
    },
    metadata: {
      index_pattern: document.getElementById("indexPattern").value || "*",
      include_system_indices: document.getElementById("includeSystemIndices")
        .checked,
      extract_mappings: document.getElementById("extractMappings").checked,
      extract_settings: document.getElementById("extractSettings").checked,
      calculate_quality_metrics: document.getElementById(
        "calculateQualityMetrics"
      ).checked,
      batch_size: parseInt(document.getElementById("batchSize").value) || 100,
      max_field_depth:
        parseInt(document.getElementById("maxFieldDepth").value) || 10,
    },
  };

  // Show loading state
  startButton.classList.add("loading");
  startButton.disabled = true;
  startButton.textContent = "Starting Extraction...";

  try {
    // Start the metadata extraction workflow
    const response = await fetch("/workflows/v1/start", {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
      },
      body: JSON.stringify(extractionConfig),
    });

    const result = await response.json();

    if (result.success) {
      resultElement.textContent = `✓ ${
        result.message || "Metadata extraction started successfully"
      }`;
      resultElement.className = "status-message success visible";

      // Show Temporal UI link
      const temporalLink = document.getElementById("temporalLink");
      if (temporalLink) {
        const temporalUrl = `http://${window.env.TEMPORAL_UI_HOST}:${window.env.TEMPORAL_UI_PORT}/namespaces/default/workflows/${result.data.workflow_id}`;
        temporalLink.href = temporalUrl;
        temporalLink.style.display = "block";
      }
    } else {
      resultElement.textContent =
        result.message || "Failed to start metadata extraction";
      resultElement.className = "status-message error visible";
    }
  } catch (error) {
    console.error("Failed to start extraction:", error);
    resultElement.textContent =
      "Failed to start metadata extraction. Please try again.";
    resultElement.className = "status-message error visible";
  } finally {
    // Reset button state
    startButton.classList.remove("loading");
    startButton.disabled = false;
    startButton.textContent = "Start Metadata Extraction";
  }
}

// Initialize the application
document.addEventListener("DOMContentLoaded", () => {
  setupAuthenticationTypeSwitch();

  // Set up event listeners
  document
    .getElementById("testConnectionBtn")
    .addEventListener("click", testConnection);
  document
    .getElementById("startExtractionBtn")
    .addEventListener("click", startMetadataExtraction);

  // Initialize page
  goToPage(1);
});
