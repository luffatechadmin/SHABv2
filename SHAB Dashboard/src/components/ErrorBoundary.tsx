import React, { Component, ErrorInfo, ReactNode } from 'react';

interface Props {
  children?: ReactNode;
}

interface State {
  hasError: boolean;
  error: Error | null;
  errorInfo: ErrorInfo | null;
}

export class ErrorBoundary extends Component<Props, State> {
  public state: State = {
    hasError: false,
    error: null,
    errorInfo: null
  };

  public static getDerivedStateFromError(error: Error): State {
    return { hasError: true, error, errorInfo: null };
  }

  public componentDidCatch(error: Error, errorInfo: ErrorInfo) {
    console.error("Uncaught error:", error, errorInfo);
    this.setState({ errorInfo });
  }

  public render() {
    if (this.state.hasError) {
      return (
        <div className="min-h-screen flex items-center justify-center bg-gray-900 text-white p-4">
          <div className="max-w-2xl w-full bg-gray-800 rounded-lg shadow-xl p-8">
            <h1 className="text-2xl font-bold text-red-500 mb-4">Something went wrong</h1>
            <div className="mb-4">
              <p className="font-semibold text-gray-300">Error:</p>
              <pre className="bg-gray-950 p-4 rounded overflow-auto text-red-400 text-sm">
                {this.state.error?.toString()}
              </pre>
            </div>
            <div>
              <p className="font-semibold text-gray-300">Stack Trace:</p>
              <pre className="bg-gray-950 p-4 rounded overflow-auto text-gray-400 text-xs h-64">
                {this.state.errorInfo?.componentStack}
              </pre>
            </div>
            <button
              className="mt-6 px-4 py-2 bg-blue-600 hover:bg-blue-700 rounded text-white transition-colors"
              onClick={() => window.location.reload()}
            >
              Reload Page
            </button>
          </div>
        </div>
      );
    }

    return this.props.children;
  }
}
