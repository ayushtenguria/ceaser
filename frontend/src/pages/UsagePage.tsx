import { useEffect, useState } from "react";
import { useSearchParams } from "react-router-dom";
import {
  MessageSquare, Database, FileText, Users, Loader2, ArrowUpRight, Crown,
  CreditCard, XCircle, CheckCircle2, Receipt,
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
  const [searchParams] = useSearchParams();
  const [planData, setPlanData] = useState<any>(null);
  const [subscription, setSubscription] = useState<any>(null);
  const [invoices, setInvoices] = useState<any[]>([]);
  const [isLoading, setIsLoading] = useState(true);
  const [upgrading, setUpgrading] = useState<string | null>(null);
  const [cancelling, setCancelling] = useState(false);
  const [showSuccess, setShowSuccess] = useState(false);

  useEffect(() => {
    if (searchParams.get("upgraded") === "true") {
      setShowSuccess(true);
      setTimeout(() => setShowSuccess(false), 5000);
    }
  }, [searchParams]);

  useEffect(() => {
    Promise.all([
      api.getMyPlan().catch(() => null),
      api.getSubscription().catch(() => null),
      api.getInvoices().catch(() => []),
    ]).then(([plan, sub, inv]) => {
      setPlanData(plan);
      setSubscription(sub);
      setInvoices(inv);
      setIsLoading(false);
    });
  }, []);

  const handleUpgrade = async (planName: string) => {
    setUpgrading(planName);
    try {
      const { checkoutUrl } = await api.createCheckout(planName);
      if (checkoutUrl) {
        window.location.href = checkoutUrl;
      }
    } catch (err: any) {
      const msg = err?.response?.data?.detail || err?.message || "Checkout failed";
      alert(msg);
    } finally {
      setUpgrading(null);
    }
  };

  const handleCancel = async () => {
    if (!confirm("Are you sure you want to cancel? You'll retain access until the end of your billing period.")) return;
    setCancelling(true);
    try {
      await api.cancelSubscription();
      setSubscription((s: any) => s ? { ...s, cancelAtPeriodEnd: true } : s);
    } catch {
      alert("Failed to cancel subscription. Please contact support.");
    } finally {
      setCancelling(false);
    }
  };

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
  const hasSub = subscription?.hasSubscription;

  return (
    <div className="p-6 max-w-5xl">
      {/* Success banner */}
      {showSuccess && (
        <div className="mb-4 flex items-center gap-2 rounded-lg border border-green-500/30 bg-green-500/10 p-3 text-sm text-green-400">
          <CheckCircle2 className="h-4 w-4" />
          Plan upgraded successfully! Your new limits are now active.
        </div>
      )}

      {/* Header */}
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
        <UsageCard icon={MessageSquare} label="Queries Today" used={usage.queriesToday.used} limit={usage.queriesToday.limit} />
        <UsageCard icon={Database} label="Connections" used={usage.connections.used} limit={usage.connections.limit} />
        <UsageCard icon={FileText} label="Reports This Month" used={usage.reportsThisMonth.used} limit={usage.reportsThisMonth.limit} />
        <UsageCard icon={Users} label="Team Members" used={usage.seats.used} limit={usage.seats.limit} />
      </div>

      {/* Active subscription info */}
      {hasSub && (
        <Card className="mt-6">
          <CardContent className="p-4">
            <div className="flex items-center justify-between">
              <div className="flex items-center gap-3">
                <CreditCard className="h-5 w-5 text-primary" />
                <div>
                  <p className="text-sm font-medium">
                    {subscription.planName.charAt(0).toUpperCase() + subscription.planName.slice(1)} Subscription
                    <Badge variant="outline" className="ml-2 text-xs">
                      {subscription.status === "active" ? "Active" : subscription.status}
                    </Badge>
                  </p>
                  <p className="text-xs text-muted-foreground">
                    via {subscription.provider?.charAt(0).toUpperCase()}{subscription.provider?.slice(1)}
                    {subscription.currentPeriodEnd && ` · Renews ${new Date(subscription.currentPeriodEnd).toLocaleDateString()}`}
                  </p>
                </div>
              </div>
              {!subscription.cancelAtPeriodEnd ? (
                <Button variant="ghost" size="sm" onClick={handleCancel} disabled={cancelling}>
                  {cancelling ? <Loader2 className="h-3.5 w-3.5 animate-spin" /> : <XCircle className="mr-1 h-3.5 w-3.5" />}
                  Cancel
                </Button>
              ) : (
                <span className="text-xs text-amber-400">Cancels at period end</span>
              )}
            </div>
          </CardContent>
        </Card>
      )}

      {/* Upgrade section */}
      {(plan === "free" || plan === "starter") && (
        <Card className="mt-6 border-primary/30 bg-primary/5">
          <CardContent className="p-6">
            <h3 className="mb-2 text-lg font-semibold">
              {plan === "free" ? "Upgrade your plan" : "Need more?"}
            </h3>
            <p className="mb-4 text-sm text-muted-foreground">
              Get more queries, connections, and features for your team.
            </p>
            <div className="grid gap-4 md:grid-cols-2">
              {plan === "free" && (
                <PlanCard
                  name="Starter"
                  price="$79"
                  color="sky"
                  features={["100 queries/day", "3 connections", "30 reports/month", "3 team members"]}
                  onUpgrade={() => handleUpgrade("starter")}
                  loading={upgrading === "starter"}
                />
              )}
              <PlanCard
                name="Business"
                price="$249"
                color="purple"
                features={["500 queries/day", "10 connections", "Unlimited reports", "10 team members"]}
                onUpgrade={() => handleUpgrade("business")}
                loading={upgrading === "business"}
                highlighted
              />
            </div>
          </CardContent>
        </Card>
      )}

      {/* Payment history */}
      {invoices.length > 0 && (
        <div className="mt-6">
          <h3 className="mb-3 flex items-center gap-2 text-sm font-medium">
            <Receipt className="h-4 w-4" /> Payment History
          </h3>
          <Card>
            <CardContent className="p-0">
              <div className="divide-y divide-border">
                {invoices.map((inv) => (
                  <div key={inv.id} className="flex items-center justify-between px-4 py-3 text-sm">
                    <div>
                      <span className="font-medium">{inv.planName.charAt(0).toUpperCase() + inv.planName.slice(1)} Plan</span>
                      <span className="ml-2 text-xs text-muted-foreground">via {inv.provider}</span>
                    </div>
                    <div className="flex items-center gap-3">
                      <span className="text-muted-foreground">
                        {new Date(inv.createdAt).toLocaleDateString()}
                      </span>
                      <span className="font-medium">
                        {inv.currency === "inr" ? "\u20B9" : "$"}{(inv.amount / 100).toFixed(2)}
                      </span>
                      <Badge variant={inv.status === "success" ? "default" : "destructive"} className="text-xs">
                        {inv.status}
                      </Badge>
                    </div>
                  </div>
                ))}
              </div>
            </CardContent>
          </Card>
        </div>
      )}
    </div>
  );
}

const PLAN_BORDER: Record<string, string> = {
  sky: "border-sky-500/30",
  purple: "border-purple-500/30",
};
const PLAN_TEXT: Record<string, string> = {
  sky: "text-sky-400",
  purple: "text-purple-400",
};

function PlanCard({ name, price, color, features, onUpgrade, loading, highlighted }: {
  name: string; price: string; color: string; features: string[];
  onUpgrade: () => void; loading: boolean; highlighted?: boolean;
}) {
  return (
    <div className={`rounded-lg border bg-card p-4 ${highlighted ? PLAN_BORDER[color] || "" : ""}`}>
      <div className="flex items-center justify-between mb-2">
        <h4 className="font-medium">{name}</h4>
        <span className={`text-lg font-bold ${PLAN_TEXT[color] || ""}`}>{price}<span className="text-xs text-muted-foreground">/mo</span></span>
      </div>
      <ul className="space-y-1 text-xs text-muted-foreground mb-3">
        {features.map((f) => <li key={f}>{f}</li>)}
      </ul>
      <Button size="sm" className="w-full" variant={highlighted ? "default" : "outline"} onClick={onUpgrade} disabled={loading}>
        {loading ? <Loader2 className="mr-1 h-3.5 w-3.5 animate-spin" /> : null}
        Upgrade <ArrowUpRight className="ml-1 h-3.5 w-3.5" />
      </Button>
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
