import { useEffect, useState } from "react";
import { useNavigate } from "react-router-dom";
import {
  MessageSquare, Database, FileText, Users, Loader2, ArrowUpRight, Crown,
} from "lucide-react";
import { Card, CardContent } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { Badge } from "@/components/ui/badge";
import * as api from "@/lib/api";

const PLAN_COLORS: Record<string, string> = {
  free: "text-muted-foreground",
  starter: "text-sky-400",
  business: "text-purple-400",
  enterprise: "text-amber-400",
};

export default function UsagePage() {
  const navigate = useNavigate();
  const [planData, setPlanData] = useState<any>(null);
  const [isLoading, setIsLoading] = useState(true);

  useEffect(() => {
    api.getMyPlan()
      .then(setPlanData)
      .catch(() => {})
      .finally(() => setIsLoading(false));
  }, []);

  if (isLoading) {
    return (
      <div className="flex h-full items-center justify-center">
        <Loader2 className="h-8 w-8 animate-spin text-primary" />
      </div>
    );
  }

  if (!planData) {
    return <div className="p-6 text-muted-foreground">Unable to load plan data.</div>;
  }

  const plan = planData.planName;
  const usage = planData.usage;

  return (
    <div className="p-6">
      <div className="mb-6 flex items-center justify-between">
        <div>
          <h2 className="text-2xl font-semibold">Usage & Plan</h2>
          <p className="text-sm text-muted-foreground">Monitor your usage and manage your subscription</p>
        </div>
        <div className="flex items-center gap-2">
          <Crown className={`h-5 w-5 ${PLAN_COLORS[plan] || ""}`} />
          <Badge variant="outline" className={`text-sm ${PLAN_COLORS[plan] || ""}`}>
            {plan.charAt(0).toUpperCase() + plan.slice(1)} Plan
          </Badge>
        </div>
      </div>

      {/* Usage cards */}
      <div className="grid gap-4 md:grid-cols-2 lg:grid-cols-4">
        <UsageCard
          icon={MessageSquare}
          label="Queries Today"
          used={usage.queriesToday.used}
          limit={usage.queriesToday.limit}
        />
        <UsageCard
          icon={Database}
          label="Connections"
          used={usage.connections.used}
          limit={usage.connections.limit}
        />
        <UsageCard
          icon={FileText}
          label="Reports This Month"
          used={usage.reportsThisMonth.used}
          limit={usage.reportsThisMonth.limit}
        />
        <UsageCard
          icon={Users}
          label="Team Members"
          used={usage.seats.used}
          limit={usage.seats.limit}
        />
      </div>

      {/* Upgrade section */}
      {plan === "free" && (
        <Card className="mt-8 border-primary/30 bg-primary/5">
          <CardContent className="p-6">
            <h3 className="mb-2 text-lg font-semibold">Upgrade your plan</h3>
            <p className="mb-4 text-sm text-muted-foreground">
              Get more queries, connections, and features for your team.
            </p>
            <div className="grid gap-4 md:grid-cols-2">
              <div className="rounded-lg border bg-card p-4">
                <div className="flex items-center justify-between mb-2">
                  <h4 className="font-medium">Starter</h4>
                  <span className="text-lg font-bold text-sky-400">$79<span className="text-xs text-muted-foreground">/mo</span></span>
                </div>
                <ul className="space-y-1 text-xs text-muted-foreground mb-3">
                  <li>100 queries/day</li>
                  <li>3 connections</li>
                  <li>30 reports/month</li>
                  <li>3 team members</li>
                </ul>
                <Button size="sm" className="w-full" variant="outline">
                  Upgrade <ArrowUpRight className="ml-1 h-3.5 w-3.5" />
                </Button>
              </div>
              <div className="rounded-lg border border-purple-500/30 bg-card p-4">
                <div className="flex items-center justify-between mb-2">
                  <h4 className="font-medium">Business</h4>
                  <span className="text-lg font-bold text-purple-400">$249<span className="text-xs text-muted-foreground">/mo</span></span>
                </div>
                <ul className="space-y-1 text-xs text-muted-foreground mb-3">
                  <li>500 queries/day</li>
                  <li>10 connections</li>
                  <li>Unlimited reports</li>
                  <li>10 team members</li>
                </ul>
                <Button size="sm" className="w-full">
                  Upgrade <ArrowUpRight className="ml-1 h-3.5 w-3.5" />
                </Button>
              </div>
            </div>
          </CardContent>
        </Card>
      )}
    </div>
  );
}

function UsageCard({ icon: Icon, label, used, limit }: {
  icon: any; label: string; used: number; limit: number;
}) {
  const pct = limit > 0 ? Math.min((used / limit) * 100, 100) : 0;
  const isNearLimit = pct >= 80;
  const isAtLimit = pct >= 100;

  return (
    <Card>
      <CardContent className="p-4">
        <div className="flex items-center gap-3 mb-3">
          <div className="flex h-9 w-9 items-center justify-center rounded-lg bg-primary/10">
            <Icon className="h-4 w-4 text-primary" />
          </div>
          <div>
            <p className="text-xs text-muted-foreground">{label}</p>
            <p className="text-lg font-bold">
              {used} <span className="text-sm font-normal text-muted-foreground">/ {limit === -1 ? "\u221E" : limit}</span>
            </p>
          </div>
        </div>
        {limit > 0 && (
          <div className="h-1.5 w-full rounded-full bg-muted">
            <div
              className={`h-full rounded-full transition-all ${
                isAtLimit ? "bg-destructive" : isNearLimit ? "bg-amber-500" : "bg-primary"
              }`}
              style={{ width: `${pct}%` }}
            />
          </div>
        )}
      </CardContent>
    </Card>
  );
}
