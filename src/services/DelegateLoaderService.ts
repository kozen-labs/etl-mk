import { BaseService } from '@kozen/engine';
import { pathToFileURL } from 'url';
import path from 'path';

type AnyDelegate = Record<string, ((...args: unknown[]) => Promise<unknown>) | undefined>;

/**
 * Utility for loading delegate modules from the filesystem.
 * Use this when you need to load a delegate outside of the Kozen IoC container.
 * For pipeline services, prefer IoC-based resolution via options.sourceDelegate / options.destinationDelegate.
 */
export class DelegateLoaderService extends BaseService {

  async loadFromFile(filePath: string, delegateType?: string): Promise<AnyDelegate> {
    const resolved = path.resolve(filePath);
    const type = delegateType ?? this.detectType(resolved);

    try {
      let mod: AnyDelegate;
      if (type === 'esm') {
        const imported = await import(pathToFileURL(resolved).href);
        mod = (imported.default ?? imported) as AnyDelegate;
      } else {
        mod = require(resolved) as AnyDelegate;
      }

      this.logger?.info({
        src: 'EtlMk:DelegateLoader:loadFromFile',
        message: 'Delegate loaded',
        data: { file: resolved, type }
      });

      return mod;
    } catch (error: unknown) {
      throw new Error(
        `Failed to load delegate from '${resolved}': ${(error as Error).message}`
      );
    }
  }

  async dispatch(delegate: AnyDelegate, event: unknown, tools: unknown, operationType?: string): Promise<unknown> {
    const specific = operationType ? delegate[operationType] : undefined;
    const fallback = delegate['message'] ?? delegate['on'] ?? delegate['default'];
    const handler  = specific ?? fallback;

    if (typeof handler !== 'function') return event;
    return handler(event, tools);
  }

  private detectType(filePath: string): 'esm' | 'cjs' {
    return path.extname(filePath) === '.mjs' ? 'esm' : 'cjs';
  }
}
