import { useEffect, useState } from "react";
import { useNavigate } from "react-router-dom";
import { Shield, Database, Key, CheckCircle2, Copy, Check } from "lucide-react";
import { Button } from "@/components/ui/button";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Badge } from "@/components/ui/badge";
import * as api from "@/lib/api";

export default function OnboardingPage() {
  const navigate = useNavigate();
  const [guide, setGuide] = useState<any>(null);
  const [copiedIdx, setCopiedIdx] = useState<string | null>(null);
  const [dbType, setDbType] = useState<"postgresql" | "mysql">("postgresql");

  useEffect(() => {
    // Fetch from API or use static
    setGuide({
      steps: [
        {
          step: 1,
          title: "Create a read-only database user",
          description: "For security, create a dedicated read-only user. This ensures Ceaser can only READ your data — never modify or delete it.",
          instructions: {
            postgresql: [
              "CREATE USER ceaser_readonly WITH PASSWORD 'your-secure-password';",
              "GRANT CONNECT ON DATABASE your_db TO ceaser_readonly;",
              "GRANT USAGE ON SCHEMA public TO ceaser_readonly;",
              "GRANT SELECT ON ALL TABLES IN SCHEMA public TO ceaser_readonly;",
              "ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT ON TABLES TO ceaser_readonly;",
            ],
            mysql: [
              "CREATE USER 'ceaser_readonly'@'%' IDENTIFIED BY 'your-secure-password';",
              "GRANT SELECT ON your_db.* TO 'ceaser_readonly'@'%';",
              "FLUSH PRIVILEGES;",
            ],
          },
        },
        {
          step: 2,
          title: "Whitelist our IP (if needed)",
          description: "If your database has firewall rules, allow connections from our server. For localhost testing, skip this.",
        },
        {
          step: 3,
          title: "Connect in Ceaser",
          description: "Go to Connections → Add Connection. Use the read-only credentials you just created.",
        },
      ],
      security: [
        "Credentials encrypted at rest (AES-256)",
        "Only SELECT queries allowed — writes blocked at 3 layers",
        "Results streamed to your browser, not stored on our servers",
        "Full audit trail of all queries",
      ],
    });
  }, []);

  const handleCopy = (text: string, key: string) => {
    navigator.clipboard.writeText(text);
    setCopiedIdx(key);
    setTimeout(() => setCopiedIdx(null), 2000);
  };

  if (!guide) return null;

  return (
    <div className="mx-auto max-w-3xl p-6">
      <div className="mb-8">
        <h2 className="text-2xl font-semibold">Setup Guide</h2>
        <p className="text-muted-foreground">Connect your database to Ceaser in 3 steps</p>
      </div>

      {/* Security banner */}
      <Card className="mb-8 border-emerald-500/30 bg-emerald-500/5">
        <CardContent className="flex items-start gap-4 p-4">
          <Shield className="mt-0.5 h-6 w-6 shrink-0 text-emerald-400" />
          <div>
            <p className="mb-2 font-medium text-emerald-400">Your data is safe</p>
            <ul className="space-y-1 text-sm text-muted-foreground">
              {guide.security.map((note: string, i: number) => (
                <li key={i} className="flex items-center gap-2">
                  <CheckCircle2 className="h-3.5 w-3.5 text-emerald-500" />
                  {note}
                </li>
              ))}
            </ul>
          </div>
        </CardContent>
      </Card>

      {/* Steps */}
      <div className="space-y-6">
        {guide.steps.map((step: any) => (
          <Card key={step.step}>
            <CardHeader className="pb-3">
              <CardTitle className="flex items-center gap-3 text-base">
                <span className="flex h-7 w-7 items-center justify-center rounded-full bg-primary text-xs font-bold text-primary-foreground">
                  {step.step}
                </span>
                {step.title}
              </CardTitle>
              <p className="text-sm text-muted-foreground">{step.description}</p>
            </CardHeader>
            {step.instructions && (
              <CardContent>
                <div className="mb-3 flex gap-2">
                  <Badge
                    variant={dbType === "postgresql" ? "default" : "outline"}
                    className="cursor-pointer"
                    onClick={() => setDbType("postgresql")}
                  >
                    PostgreSQL
                  </Badge>
                  <Badge
                    variant={dbType === "mysql" ? "default" : "outline"}
                    className="cursor-pointer"
                    onClick={() => setDbType("mysql")}
                  >
                    MySQL
                  </Badge>
                </div>
                <div className="space-y-2">
                  {step.instructions[dbType]?.map((cmd: string, i: number) => {
                    const key = `${step.step}-${i}`;
                    return (
                      <div key={i} className="group flex items-start gap-2 rounded-md bg-muted/50 px-3 py-2">
                        <code className="flex-1 text-xs text-emerald-400">{cmd}</code>
                        <button
                          onClick={() => handleCopy(cmd, key)}
                          className="shrink-0 text-muted-foreground hover:text-foreground"
                        >
                          {copiedIdx === key ? <Check className="h-3.5 w-3.5" /> : <Copy className="h-3.5 w-3.5" />}
                        </button>
                      </div>
                    );
                  })}
                </div>
              </CardContent>
            )}
          </Card>
        ))}
      </div>

      <div className="mt-8 flex justify-center">
        <Button size="lg" onClick={() => navigate("/connections")}>
          <Database className="mr-2 h-4 w-4" />
          Connect Your Database
        </Button>
      </div>
    </div>
  );
}
