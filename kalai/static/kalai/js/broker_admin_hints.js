/**
 * Dynamic Multi-Broker Credential Hints for Django Admin
 * Automatically updates input placeholders and inline help message boxes in real-time.
 */
document.addEventListener("DOMContentLoaded", function () {
    const BROKER_HINT_MAP = {
        "kotak": {
            displayName: "Kotak Neo",
            badge: "Kotak Neo (Trade API v2 Direct 2FA)",
            account_id: {
                placeholder: "e.g. W1NPY (Kotak Neo Client ID / UCC)",
                hint: "<strong>Account ID (Client ID / UCC):</strong> Kotak Neo Client ID / UCC (e.g. <code>W1NPY</code>)."
            },
            api_key: {
                placeholder: "Paste Consumer Key / Customer Key from Kotak Developer Portal",
                hint: "<strong>API Key (Consumer Key):</strong> Paste Consumer Key from Kotak Neo Developer Portal (tradeapi.kotaksecurities.com / neo.kotaksecurities.com)."
            },
            api_secret: {
                placeholder: "Enter 6-digit Kotak Neo MPIN (e.g. 123456)",
                hint: "<strong>API Secret (Neo MPIN):</strong> Enter your 6-digit Kotak Neo login MPIN (e.g. <code>123456</code>)."
            },
            totp_secret: {
                placeholder: "Enter 32-character TOTP 2FA Secret Key from Kotak Neo portal",
                hint: "<strong>TOTP 2FA Secret:</strong> 32-character base32 TOTP secret key from Kotak Neo Trade API portal for automated 2FA token generation."
            },
            refresh_token: {
                placeholder: "Enter 10-digit Registered Mobile No. (e.g. +919876543210)",
                hint: "<strong>Mobile Number:</strong> Registered Kotak Neo mobile number with country code (e.g. <code>+919876543210</code>)."
            },
            access_token: {
                placeholder: "Bearer Session Token <token>:::<sid> (auto-updated on login)",
                hint: "<strong>Access Token:</strong> Daily session bearer token (<code>&lt;token&gt;:::&lt;sid&gt;</code>), automatically generated upon login."
            }
        },
        "zerodha": {
            displayName: "Zerodha Kite",
            badge: "Zerodha Kite Connect (OAuth 2.0 + TOTP)",
            account_id: {
                placeholder: "e.g. HS6525 (Kite User ID)",
                hint: "<strong>Account ID:</strong> 6-character Zerodha Kite User ID (e.g. <code>HS6525</code>)."
            },
            api_key: {
                placeholder: "Paste Kite Connect API Key",
                hint: "<strong>API Key:</strong> API Key from kite.trade developer console."
            },
            api_secret: {
                placeholder: "Paste Kite Connect API Secret",
                hint: "<strong>API Secret:</strong> API Secret from kite.trade developer console."
            },
            totp_secret: {
                placeholder: "32-character base32 TOTP 2FA secret key",
                hint: "<strong>TOTP Secret:</strong> 32-character base32 TOTP secret seed for automated daily 2FA login."
            },
            refresh_token: {
                placeholder: "Not required for Zerodha Kite",
                hint: "<strong>Refresh Token:</strong> Not required for Zerodha Kite Connect."
            },
            access_token: {
                placeholder: "Daily OAuth Access Token (auto-updated on login)",
                hint: "<strong>Access Token:</strong> Daily OAuth session token, auto-updated upon login."
            }
        },
        "coindcx": {
            displayName: "CoinDCX",
            badge: "CoinDCX (HMAC-SHA256)",
            account_id: {
                placeholder: "e.g. PR45134584 or registered email",
                hint: "<strong>Account ID:</strong> User account identifier or email."
            },
            api_key: {
                placeholder: "Paste CoinDCX API Key",
                hint: "<strong>API Key:</strong> Public API Key from CoinDCX."
            },
            api_secret: {
                placeholder: "Paste CoinDCX API Secret (HMAC-SHA256)",
                hint: "<strong>API Secret:</strong> Secret key used for HMAC-SHA256 request signing."
            },
            totp_secret: {
                placeholder: "Not required",
                hint: "<strong>TOTP Secret:</strong> Not required for CoinDCX API."
            },
            refresh_token: {
                placeholder: "Not required",
                hint: "<strong>Refresh Token:</strong> Not required for CoinDCX."
            },
            access_token: {
                placeholder: "Not required",
                hint: "<strong>Access Token:</strong> Not required (HMAC signature used per request)."
            }
        },
        "coinswitch": {
            displayName: "CoinSwitch PRO",
            badge: "CoinSwitch PRO (Ed25519 / HMAC)",
            account_id: {
                placeholder: "e.g. CS_USER_01 or Account ID",
                hint: "<strong>Account ID:</strong> CoinSwitch account identifier."
            },
            api_key: {
                placeholder: "Paste CoinSwitch PRO API Key",
                hint: "<strong>API Key:</strong> PRO API Key from CoinSwitch portal."
            },
            api_secret: {
                placeholder: "Paste Secret Key (Ed25519 seed - Valid for 90 days)",
                hint: "<strong>API Secret:</strong> 64-character hex private key (Ed25519 seed). <span style='color:#d97706;font-weight:600;'>⚠️ CoinSwitch secret keys expire after 90 days. The system will alert you 7 days before expiry.</span>"
            },
            totp_secret: {
                placeholder: "Not required",
                hint: "<strong>TOTP Secret:</strong> Not required."
            },
            refresh_token: {
                placeholder: "Not required",
                hint: "<strong>Refresh Token:</strong> Not required for CoinSwitch PRO."
            },
            access_token: {
                placeholder: "Optional session token",
                hint: "<strong>Access Token:</strong> Optional session token."
            }
        },
        "tradovate": {
            displayName: "Tradovate",
            badge: "Tradovate API",
            account_id: {
                placeholder: "e.g. tradovate_username",
                hint: "<strong>Account ID:</strong> Tradovate username."
            },
            api_key: {
                placeholder: "Paste CID / App ID",
                hint: "<strong>API Key:</strong> Client ID / App ID from Tradovate."
            },
            api_secret: {
                placeholder: "Paste App Secret / Password",
                hint: "<strong>API Secret:</strong> Application Secret or master password."
            },
            totp_secret: {
                placeholder: "Optional 2FA seed",
                hint: "<strong>TOTP Secret:</strong> Optional 2FA seed."
            },
            refresh_token: {
                placeholder: "Not required",
                hint: "<strong>Refresh Token:</strong> Not required for Tradovate."
            },
            access_token: {
                placeholder: "Bearer Session Token",
                hint: "<strong>Access Token:</strong> Session bearer token."
            }
        }
    };

    const brokerSelect = document.querySelector("#id_broker_name");
    const providerSelect = document.querySelector("#id_api_provider");

    if (!brokerSelect && !providerSelect) {
        return;
    }

    // Insert top link bar if on change/add form
    const form = document.querySelector("#broker_form") || document.querySelector("form");
    if (form && !document.querySelector(".broker-guide-link-bar")) {
        const bar = document.createElement("div");
        bar.className = "broker-guide-link-bar";
        const currentPath = window.location.pathname;
        const brokerBaseMatch = currentPath.match(/^(.*\/kalai\/broker\/)/);
        const guideUrl = brokerBaseMatch ? (brokerBaseMatch[1] + "credential-guide/") : "../credential-guide/";
        bar.innerHTML = '<span>💡 <strong>Multi-Broker Setup:</strong> Need help mapping credentials for Kotak Neo, Zerodha, or Crypto?</span><a href="' + guideUrl + '" target="_blank">View Multi-Broker Setup Guide &nearr;</a>';
        form.insertBefore(bar, form.firstChild);
    }

    function getSelectedBrokerKey() {
        let text = "";
        if (brokerSelect) {
            const opt = brokerSelect.options[brokerSelect.selectedIndex];
            if (opt && opt.value) {
                text += " " + opt.text.toLowerCase() + " " + opt.value.toLowerCase();
            }
        }
        if (providerSelect) {
            const opt = providerSelect.options[providerSelect.selectedIndex];
            if (opt && opt.value) {
                text += " " + opt.text.toLowerCase() + " " + opt.value.toLowerCase();
            }
        }

        if (text.includes("kotak") || text.includes("neo")) return "kotak";
        if (text.includes("zerodha") || text.includes("kite")) return "zerodha";
        if (text.includes("coindcx")) return "coindcx";
        if (text.includes("coinswitch")) return "coinswitch";
        if (text.includes("tradovate")) return "tradovate";
        return null;
    }

    function updateBadge(badgeText) {
        const header = document.querySelector(".module h2, fieldset.module h2, #fieldset-0-1-heading, #fieldset-0-0-heading");
        if (!header) return;

        let badge = document.querySelector("#broker-active-badge");
        if (!badge) {
            badge = document.createElement("span");
            badge.id = "broker-active-badge";
            badge.className = "broker-active-badge";
            header.appendChild(badge);
        }
        if (badgeText) {
            badge.style.display = "inline-flex";
            badge.innerHTML = badgeText;
        } else {
            badge.style.display = "none";
        }
    }

    function applyHints() {
        const brokerKey = getSelectedBrokerKey();
        const config = brokerKey ? BROKER_HINT_MAP[brokerKey] : null;

        updateBadge(config ? config.badge : null);

        const targetFields = ["account_id", "api_key", "api_secret", "totp_secret", "refresh_token", "access_token"];

        targetFields.forEach(function (fieldId) {
            const field = document.querySelector("#id_" + fieldId);
            if (!field) return;

            // 1. Update native Django help text box if present
            const helpEl = document.querySelector("#id_" + fieldId + "_helptext") || 
                           (field.closest(".form-row") ? field.closest(".form-row").querySelector(".help") : null);

            // 2. Also ensure custom styled hint element is updated
            let customHint = document.querySelector(".broker-field-hint[data-field='" + fieldId + "']");
            if (!customHint) {
                customHint = document.createElement("div");
                customHint.className = "broker-field-hint";
                customHint.setAttribute("data-field", fieldId);
                field.parentNode.insertBefore(customHint, field.nextSibling);
            }

            if (config && config[fieldId]) {
                const fieldConf = config[fieldId];
                field.setAttribute("placeholder", fieldConf.placeholder);
                
                if (helpEl) {
                    helpEl.style.display = "none";
                }
                customHint.innerHTML = fieldConf.hint;
                customHint.style.display = "flex";
            } else {
                if (helpEl) {
                    helpEl.style.display = "block";
                }
                customHint.style.display = "none";
            }
        });

        // 3. Dynamic Secret Key Expiry Section Handling
        updateExpiryFieldsVisibility(brokerKey);
    }

    const expiryToggle = document.querySelector("#id_enable_secret_key_expiry");

    function updateExpiryFieldsVisibility(brokerKey) {
        if (!expiryToggle) return;

        const isAddPage = window.location.pathname.includes("/add/");
        if (isAddPage && brokerKey !== undefined) {
            // Auto-check for CoinSwitch, uncheck for other brokers if on Add form
            if (brokerKey === "coinswitch") {
                expiryToggle.checked = true;
            } else if (brokerKey) {
                expiryToggle.checked = false;
            }
        }

        const isEnabled = expiryToggle.checked;
        const dependentFieldIds = ["secret_key_validity_days", "secret_key_warn_days", "api_secret_updated_at"];

        dependentFieldIds.forEach(function (fieldId) {
            const field = document.querySelector("#id_" + fieldId) || document.querySelector(".field-" + fieldId);
            if (!field) return;
            const row = field.closest(".form-row") || field.closest(".form-group") || field;
            if (row) {
                row.style.display = isEnabled ? "" : "none";
            }
        });
    }

    if (expiryToggle) {
        expiryToggle.addEventListener("change", function () {
            updateExpiryFieldsVisibility();
        });
    }

    if (brokerSelect) {
        brokerSelect.addEventListener("change", applyHints);
    }
    if (providerSelect) {
        providerSelect.addEventListener("change", applyHints);
    }

    // Initial load
    applyHints();
    updateExpiryFieldsVisibility();
});

