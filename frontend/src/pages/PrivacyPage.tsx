import { BarChart3 } from "lucide-react";
import { useNavigate } from "react-router-dom";

export default function PrivacyPage() {
  const navigate = useNavigate();
  return (
    <div className="min-h-screen bg-background">
      <nav className="border-b bg-background/80 backdrop-blur-sm">
        <div className="mx-auto flex h-16 max-w-4xl items-center gap-3 px-6">
          <button onClick={() => navigate("/")} className="flex items-center gap-2 font-bold">
            <BarChart3 className="h-5 w-5 text-primary" />
            Ceaser
          </button>
        </div>
      </nav>
      <div className="mx-auto max-w-3xl px-6 py-12 prose prose-invert">
        <h1>Privacy Policy</h1>
        <p><em>Last updated: March 2026</em></p>

        <h2>1. Information We Collect</h2>
        <p><strong>Account data:</strong> Email, name, organization (via Clerk authentication).</p>
        <p><strong>Usage data:</strong> Queries submitted, features used, timestamps (for audit logs).</p>
        <p><strong>Connection credentials:</strong> Database host, port, username, encrypted password (AES-256 Fernet encryption at rest).</p>

        <h2>2. Information We Do NOT Collect</h2>
        <p><strong>Your database data:</strong> Query results stream directly to your browser. We do not store, index, or analyze your business data unless you explicitly save a report.</p>
        <p><strong>File contents:</strong> Uploaded Excel/CSV files are processed in memory and stored as optimized parquet files for querying. They are deleted when you remove the file.</p>

        <h2>3. How We Use Information</h2>
        <p>Account data: authentication and authorization. Usage data: improving the service, enforcing plan limits, audit compliance. Connection credentials: connecting to your databases on your behalf.</p>

        <h2>4. Data Security</h2>
        <ul>
          <li>Database credentials encrypted at rest (AES-256 Fernet)</li>
          <li>All API communication over HTTPS/TLS</li>
          <li>Read-only database access enforced at 3 layers</li>
          <li>JWT-based authentication via Clerk</li>
          <li>Role-based access control (RBAC)</li>
          <li>Full audit trail of all queries</li>
        </ul>

        <h2>5. Data Retention</h2>
        <p>Conversation history: retained per your plan (7 days free, 90 days starter, 1 year business). Audit logs: retained for 1 year. You can delete your account and all associated data at any time.</p>

        <h2>6. Third-Party Services</h2>
        <p>We use: Clerk (authentication), Google Gemini/Anthropic Claude (AI processing — your queries are sent to LLM APIs), Neon (application database). We do not sell or share your data with any other third parties.</p>

        <h2>7. Your Rights</h2>
        <p>You have the right to: access your data, request deletion, export your data, and opt out of non-essential data collection. Contact us to exercise these rights.</p>

        <h2>8. GDPR & CCPA</h2>
        <p>We comply with GDPR and CCPA requirements. For EU users, the legal basis for processing is contract performance and legitimate interest.</p>

        <h2>Contact</h2>
        <p>Data Protection Officer: privacy@ceaser.ai</p>
      </div>
    </div>
  );
}
