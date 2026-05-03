import { BaseService } from '@kozen/engine';
import { pathToFileURL } from 'url';
import path from 'path';

type AnyDelegate = Record<string, ((...args: unknown[]) => Promise<unknown>) | undefined>;

export class DelegateLoaderService extends BaseService {
  private delegate: AnyDelegate | null = null;

  async load(filePath?: string, delegateType?: string): Promise<void> {
    if (!filePath) {
      this.delegate = null;
      return;
    }

    const resolved = path.resolve(filePath);
    const type = delegateType ?? this.detectType(resolved);

    try {
      if (type === 'esm') {
        const mod = await import(pathToFileURL(resolved).href);
        this.delegate = (mod.default ?? mod) as AnyDelegate;
      } else {
        this.delegate = require(resolved) as AnyDelegate;
      }

      this.logger?.info({
        src: 'EtlMk:DelegateLoader:load',
        message: `Delegate loaded`,
        data: { file: resolved, type }
      });
    } catch (error: unknown) {
      throw new Error(
        `Failed to load delegate from '${resolved}': ${(error as Error).message}`
      );
    }
  }

  async dispatch(event: unknown, tools: unknown, operationType?: string): Promise<unknown> {
    if (!this.delegate) {
      return this.passthrough(event, !!operationType);
    }

    const specific = operationType ? this.delegate[operationType] : undefined;
    const fallback =
      this.delegate.message ??
      this.delegate.on ??
      this.delegate.default;

    const handler = specific ?? fallback;

    if (typeof handler !== 'function') {
      return this.passthrough(event, !!operationType);
    }

    return handler(event, tools);
  }

  private passthrough(event: unknown, isMongo: boolean): unknown {
    if (isMongo) {
      const e = event as Record<string, unknown>;
      return e['fullDocument'] ?? e;
    }
    return event;
  }

  private detectType(filePath: string): 'esm' | 'cjs' {
    const ext = path.extname(filePath);
    if (ext === '.mjs') return 'esm';
    return 'cjs';
  }
}
