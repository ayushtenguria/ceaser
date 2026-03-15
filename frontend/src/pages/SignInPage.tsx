import { SignIn } from "@clerk/clerk-react";
import { Navigate } from "react-router-dom";

const clerkEnabled = !!import.meta.env.VITE_CLERK_PUBLISHABLE_KEY;

export default function SignInPage() {
  if (!clerkEnabled) return <Navigate to="/chat" replace />;

  return (
    <div className="flex min-h-screen items-center justify-center bg-background">
      <SignIn
        routing="path"
        path="/sign-in"
        signUpUrl="/sign-up"
        appearance={{
          elements: {
            rootBox: "mx-auto",
            card: "bg-card border border-border shadow-xl",
          },
        }}
      />
    </div>
  );
}
