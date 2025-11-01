# in app/tools.py
import joblib
import pandas as pd
import shap
import json
from crewai_tools import tool
from typing import List

# --- 1. DEFINE YOUR MODEL/DATA CONSTANTS ---

# TODO: YOU MUST UPDATE THESE PATHS
MODEL_DIR = './models/'
ANOMALY_MODEL_PATH = f"{MODEL_DIR}anomaly_model.joblib"
ANOMALY_SCALER_PATH = f"{MODEL_DIR}anomaly_scaler.joblib"
SUPERVISED_MODEL_PATH = f"{MODEL_DIR}supervised_model.joblib"
SUPERVISED_SCALER_PATH = f"{MODEL_DIR}supervised_scaler.joblib"
LABEL_ENCODER_PATH = f"{MODEL_DIR}label_encoder.joblib"
MOCK_DB_PATH = './data/mock_threat_db.json'

# TODO: YOU MUST UPDATE THIS LIST
# This list MUST match the column names your models were trained on, 
# in the exact order.
FEATURE_NAMES = [
    'Destination Port', 'Flow Duration', 'Total Fwd Packets', 
    'Total Backward Packets', 'Total Length of Fwd Packets', 
    'Total Length of Bwd Packets', 'Fwd Packet Length Max', 
    'Fwd Packet Length Min', 'Fwd Packet Length Mean', 'Fwd Packet Length Std',
    # ... ADD ALL OTHER 70+ FEATURES HERE ...
    'Active Mean', 'Active Std', 'Active Max', 'Active Min', 'Idle Mean', 
    'Idle Std', 'Idle Max', 'Idle Min'
]

# TODO: YOU MUST UPDATE THESE
# These are the column names in your CSV for the IPs
SRC_IP_COLUMN = 'Source IP'
DST_IP_COLUMN = 'Destination IP'


# --- 2. LOAD MODELS AND SCALERS ONCE ---

try:
    print("Loading models and scalers...")
    ANOMALY_MODEL = joblib.load(ANOMALY_MODEL_PATH)
    ANOMALY_SCALER = joblib.load(ANOMALY_SCALER_PATH)
    SUPERVISED_MODEL = joblib.load(SUPERVISED_MODEL_PATH)
    SUPERVISED_SCALER = joblib.load(SUPERVISED_SCALER_PATH)
    LABEL_ENCODER = joblib.load(LABEL_ENCODER_PATH)
    
    with open(MOCK_DB_PATH, 'r') as f:
        MOCK_THREAT_DB = json.load(f)
    print("All models and mock DB loaded successfully.")
    
except FileNotFoundError as e:
    print(f"Error: Could not find a required file. {e}")
    print("Please check your file paths in /models/ and /data/ directories.")
    exit(1)
except Exception as e:
    print(f"An error occurred during model loading: {e}")
    exit(1)

# --- 3. INITIALIZE SHAP EXPLAINER ---

try:
    # We create the explainer for the supervised model
    print("Initializing SHAP Explainer...")
    SHAP_EXPLAINER = shap.TreeExplainer(SUPERVISED_MODEL)
    print("SHAP Explainer initialized.")
except Exception as e:
    print(f"Warning: Could not initialize SHAP TreeExplainer. {e}")
    print("Is your supervised model (e.g., RandomForest, XGBoost) tree-based?")
    SHAP_EXPLAINER = None

# --- 4. HELPER FUNCTION ---

def _process_log_to_features(network_log_json: str, scaler_type: str) -> pd.DataFrame:
    """
    Internal helper to convert a JSON log string into a scaled DataFrame 
    ready for model prediction.
    """
    # 1. Parse the JSON string
    log_data = json.loads(network_log_json)
    
    # 2. Convert to DataFrame, ensuring all columns are present
    df = pd.DataFrame([log_data])
    df = df.reindex(columns=FEATURE_NAMES, fill_value=0)
    
    # 3. Select the correct scaler
    if scaler_type == 'anomaly':
        scaler = ANOMALY_SCALER
    elif scaler_type == 'supervised':
        scaler = SUPERVISED_SCALER
    else:
        raise ValueError("Invalid scaler_type specified.")
        
    # 4. Apply the scaler
    scaled_features = scaler.transform(df)
    
    # 5. Return as a named DataFrame (important for SHAP)
    return pd.DataFrame(scaled_features, columns=FEATURE_NAMES)

# --- 5. DEFINE AGENT TOOLS ---

@tool("Anomaly Detection Tool")
def anomaly_tool(network_log_json: str) -> str:
    """
    Takes a single network log as a JSON string.
    Runs an Isolation Forest model (watchdog.joblib) to return 'anomaly' or 'benign'.
    This is the first check to see if the log is suspicious.
    """
    try:
        features_df = _process_log_to_features(network_log_json, 'anomaly')
        prediction = ANOMALY_MODEL.predict(features_df)
        return "anomaly" if prediction[0] == -1 else "benign"
    except Exception as e:
        return f"Error in anomaly_tool: {e}"

@tool("Threat Classification Tool")
def classification_tool(network_log_json: str) -> str:
    """
    Takes a single network log as a JSON string.
    Runs a Supervised ML model to classify the specific attack type.
    Uses the label_encoder to return the human-readable attack name.
    """
    try:
        features_df = _process_log_to_features(network_log_json, 'supervised')
        
        # Predict the class index (e.g., [0], [1], [4])
        prediction_index = SUPERVISED_MODEL.predict(features_df)
        
        # Convert the index back to the original label (e.g., "DDoS", "Benign")
        class_name = LABEL_ENCODER.inverse_transform(prediction_index)
        
        return str(class_name[0])
    except Exception as e:
        return f"Error in classification_tool: {e}"

@tool("Simulated CTI Enrichment Tool")
def simulated_enrichment_tool(network_log_json: str) -> str:
    """
    Takes the network log JSON. It extracts the Source and Destination IPs
    and checks them against a local 'mock_threat_db.json' file.
    Returns any CTI information found.
    """
    try:
        log_data = json.loads(network_log_json)
        src_ip = log_data.get(SRC_IP_COLUMN)
        dst_ip = log_data.get(DST_IP_COLUMN)
        
        findings = []
        if src_ip in MOCK_THREAT_DB:
            info = MOCK_THREAT_DB[src_ip]
            findings.append(f"Source IP ({src_ip}): [Threat: {info['threat']}] {info['details']}")
        
        if dst_ip in MOCK_THREAT_DB:
            info = MOCK_THREAT_DB[dst_ip]
            findings.append(f"Destination IP ({dst_ip}): [Threat: {info['threat']}] {info['details']}")
            
        if not findings:
            return "No CTI information found for associated IPs."
            
        return " ".join(findings)
    except Exception as e:
        return f"Error in simulated_enrichment_tool: {e}"

@tool("Explain Prediction Tool")
def explanation_tool(network_log_json: str) -> str:
    """
    Explains *why* the supervised ML model made its prediction.
    Uses SHAP to find and return the top 3 features contributing to the decision.
    """
    if not SHAP_EXPLAINER:
        return "XAI Explanation is not available (SHAP Explainer not initialized)."
        
    try:
        features_df = _process_log_to_features(network_log_json, 'supervised')
        
        # Get the index of the class that was predicted
        prediction_index = SUPERVISED_MODEL.predict(features_df)[0]
        
        # Get SHAP values
        shap_values = SHAP_EXPLAINER.shap_values(features_df)
        
        # Get the SHAP values for the *specific class that was predicted*
        class_shap_values = shap_values[prediction_index][0]
        
        # Create a DataFrame of features and their SHAP values
        shap_df = pd.DataFrame({
            'feature': FEATURE_NAMES,
            'shap_value': class_shap_values
        })
        
        # Get top 3 features by *absolute* SHAP value
        top_3_features = shap_df.reindex(shap_df.shap_value.abs().sort_values(ascending=False).index).head(3)
        
        # Format the explanation
        explanation = "Prediction was primarily driven by: "
        feature_list = []
        for _, row in top_3_features.iterrows():
            feature_list.append(f"{row['feature']} (impact: {row['shap_value']:.2f})")
        
        return explanation + ", ".join(feature_list)
        
    except Exception as e:
        return f"Error generating SHAP explanation: {e}"