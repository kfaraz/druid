/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import { createStore } from 'zustand';

import type { Highlight } from '../models';

interface HighlightState {
  /**
   * The current highlight.
   */
  highlight: Highlight | undefined;

  /**
   * Sets the highlight.
   * @param highlight the highlight to set
   */
  setHighlight: (highlight: Highlight) => void;

  /**
   * Drops the highlight.
   */
  dropHighlight: () => void;

  /**
   * Updates the highlight.
   * @param highlight the highlight to update (only the properties to update)
   * @returns the updated highlight, or undefined if there's no highlight in the store
   */
  updateHighlight: (highlight: Partial<Highlight>) => Highlight | undefined;
}

/**
 * A lightweight store for the highlight.
 */
export const highlightStore = createStore<HighlightState>((set, get) => ({
  highlight: undefined,
  setHighlight: highlight => set({ highlight }),
  dropHighlight: () => set({ highlight: undefined }),
  updateHighlight: highlight => {
    set(state => {
      if (!state.highlight) return state;

      return { highlight: { ...state.highlight, ...highlight } };
    });

    return get().highlight;
  },
}));
