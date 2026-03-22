import { BarChart3 } from "lucide-react";
import { useNavigate } from "react-router-dom";
import { Badge } from "@/components/ui/badge";

const CHANGELOG = [
  {
    version: "1.0.0",
    date: "March 2026",
    title: "Initial Launch",
    changes: [
      { type: "feature", text: "Natural language to SQL queries with 99%+ accuracy" },
      { type: "feature", text: "Excel/CSV file upload with 7-agent processing pipeline" },
      { type: "feature", text: "Cross-database queries for microservice architectures" },
      { type: "feature", text: "Automated report generation with PDF export" },
      { type: "feature", text: "Reusable notebooks extracted from conversations" },
      { type: "feature", text: "Semantic layer (business metric definitions)" },
      { type: "feature", text: "Data Analyst Agent for strategic questions" },
      { type: "feature", text: "Smart follow-up suggestions after every response" },
      { type: "feature", text: "Multi-tenant with RBAC (super admin, admin, member, viewer)" },
      { type: "security", text: "3-layer read-only enforcement on all database queries" },
      { type: "security", text: "Encrypted credentials (AES-256 Fernet)" },
      { type: "security", text: "Full audit trail of all queries" },
    ],
  },
];

export default function ChangelogPage() {
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
      <div className="mx-auto max-w-3xl px-6 py-12">
        <h1 className="mb-8 text-3xl font-bold">Changelog</h1>

        {CHANGELOG.map((release) => (
          <div key={release.version} className="mb-12">
            <div className="mb-4 flex items-center gap-3">
              <Badge className="text-sm">{release.version}</Badge>
              <span className="text-sm text-muted-foreground">{release.date}</span>
            </div>
            <h2 className="mb-4 text-xl font-semibold">{release.title}</h2>
            <ul className="space-y-2">
              {release.changes.map((change, i) => (
                <li key={i} className="flex items-start gap-2 text-sm">
                  <Badge variant={change.type === "feature" ? "default" : change.type === "security" ? "secondary" : "outline"} className="mt-0.5 text-[10px] shrink-0">
                    {change.type}
                  </Badge>
                  <span className="text-muted-foreground">{change.text}</span>
                </li>
              ))}
            </ul>
          </div>
        ))}
      </div>
    </div>
  );
}
