import React from "react";
import ReactDOM from "react-dom/client";
import { BrowserRouter } from "react-router-dom";
import { ClerkProvider } from "@clerk/clerk-react";
import { dark } from "@clerk/themes";
import App from "@/App";
import "@/index.css";

const CLERK_PUBLISHABLE_KEY = import.meta.env.VITE_CLERK_PUBLISHABLE_KEY;

function Root() {
  if (CLERK_PUBLISHABLE_KEY) {
    return (
      <ClerkProvider
        publishableKey={CLERK_PUBLISHABLE_KEY}
        appearance={{ baseTheme: dark }}
      >
        <BrowserRouter>
          <App />
        </BrowserRouter>
      </ClerkProvider>
    );
  }

  return (
    <BrowserRouter>
      <App />
    </BrowserRouter>
  );
}

ReactDOM.createRoot(document.getElementById("root")!).render(
  <React.StrictMode>
    <Root />
  </React.StrictMode>
);
