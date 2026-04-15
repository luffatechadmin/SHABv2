/**
 * Environment Variable Validation
 * 
 * This module validates that all required environment variables
 * are properly configured before the application starts.
 */

interface EnvConfig {
  VITE_SUPABASE_URL: string;
  VITE_SUPABASE_ANON_KEY: string;
  VITE_SITE_URL?: string;
  VITE_SITE_NAME?: string;
}

interface ValidationResult {
  isValid: boolean;
  errors: string[];
  warnings: string[];
  config: Partial<EnvConfig>;
}

/**
 * Validates all required environment variables are set
 */
export function validateEnv(): ValidationResult {
  const errors: string[] = [];
  const warnings: string[] = [];
  
  const config: Partial<EnvConfig> = {
    VITE_SUPABASE_URL: import.meta.env.VITE_SUPABASE_URL,
    VITE_SUPABASE_ANON_KEY: import.meta.env.VITE_SUPABASE_ANON_KEY,
    VITE_SITE_URL: import.meta.env.VITE_SITE_URL,
    VITE_SITE_NAME: import.meta.env.VITE_SITE_NAME,
  };

  // Required variables
  if (!config.VITE_SUPABASE_URL) {
    errors.push('VITE_SUPABASE_URL is required');
  } else if (!isValidUrl(config.VITE_SUPABASE_URL)) {
    errors.push('VITE_SUPABASE_URL must be a valid URL');
  }

  if (!config.VITE_SUPABASE_ANON_KEY) {
    errors.push('VITE_SUPABASE_ANON_KEY is required');
  } else if (!config.VITE_SUPABASE_ANON_KEY.startsWith('eyJ') && !config.VITE_SUPABASE_ANON_KEY.startsWith('sb_')) {
    warnings.push('VITE_SUPABASE_ANON_KEY may not be a valid Supabase key');
  }

  // Optional variables with warnings
  if (!config.VITE_SITE_URL) {
    warnings.push('VITE_SITE_URL is not set - SEO features may not work correctly');
  }

  if (!config.VITE_SITE_NAME) {
    warnings.push('VITE_SITE_NAME is not set - using default site name');
  }

  return {
    isValid: errors.length === 0,
    errors,
    warnings,
    config,
  };
}

/**
 * Validates a URL string
 */
function isValidUrl(urlString: string): boolean {
  try {
    new URL(urlString);
    return true;
  } catch {
    return false;
  }
}

/**
 * Logs validation results to console
 */
export function logValidationResults(result: ValidationResult): void {
  if (result.errors.length > 0) {
    console.error('🚨 Environment Configuration Errors:');
    result.errors.forEach(error => console.error(`  ❌ ${error}`));
  }

  if (result.warnings.length > 0) {
    console.warn('⚠️ Environment Configuration Warnings:');
    result.warnings.forEach(warning => console.warn(`  ⚠️ ${warning}`));
  }

  if (result.isValid && result.warnings.length === 0) {
    // Validation successful
  }
}

/**
 * Initializes environment validation
 * Call this at app startup
 */
export function initEnvValidation(): void {
  const result = validateEnv();
  
  // Always log in development
  if (import.meta.env.DEV) {
    logValidationResults(result);
  }
  
  // In production, only log errors
  if (import.meta.env.PROD && !result.isValid) {
    logValidationResults(result);
    
    // Optionally throw an error to prevent app from running with invalid config
    // Uncomment the line below to enforce this:
    // throw new Error('Invalid environment configuration. Check console for details.');
  }
}

/**
 * Get site configuration from environment
 */
export function getSiteConfig() {
  return {
    url: import.meta.env.VITE_SITE_URL || '',
    name: import.meta.env.VITE_SITE_NAME || 'S.H.A.B',
    supabaseUrl: import.meta.env.VITE_SUPABASE_URL,
    supabaseKey: import.meta.env.VITE_SUPABASE_ANON_KEY,
  };
}
