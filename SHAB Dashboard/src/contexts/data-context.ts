import { createContext } from "react";

import type { DataContextType } from "./master-data-types";

export const DataContext = createContext<DataContextType | undefined>(undefined);
